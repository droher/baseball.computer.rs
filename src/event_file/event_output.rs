
use chrono::{Date, NaiveDate, NaiveTime};
use serde::{Deserialize, Serialize};

use crate::event_file::info::{DayNight, DoubleheaderStatus, FieldCondition, HowScored, Precipitation, Sky, UmpirePosition, WindDirection};
use crate::event_file::parser::Matchup;
use crate::event_file::pitch_sequence::{Pitch, PitchSequence};
use crate::event_file::play::{Base, BaseRunner, BaserunningPlayType, Count, HitLocation, InningFrame, PlateAppearanceType};
use crate::event_file::traits::{BattingStats, DefenseStats, FieldingPlayType, FieldingPosition, GameFileStatus, GameType, Handedness, LineupPosition, PitchingStats, Side};

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize)]
enum EventInfoType {}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct Season(u16);

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct League(String);

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct Franchise {
    retrosheet_id: String,
    franchise_name: String
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct Division {
    league: League,
    division_name: String
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct City {
    city_name: Option<String>,
    state_name: Option<String>,
    country_name: String
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct Person {
    date_of_birth: Option<NaiveDate>,
    date_of_death: Option<NaiveDate>,
    bats: Handedness,
    throws: Handedness,
    // Provide default here if birth_date is populated
    birth_year: Option<u16>,
    weight_pounds: Option<u16>,
    height_inches: Option<u16>,
    place_of_birth: City,
    place_of_death: City,
    retrosheet_id: String,
    full_name: String
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct Park {
    park_name: String,
    alias: String,
    retrosheet_id: String,
    city: City
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct FranchiseSeason {
    franchise: Franchise,
    season: Season,
    division: Division
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct GameSetting {
    start_time: NaiveTime,
    doubleheader_status: DoubleheaderStatus,
    time_of_day: DayNight,
    game_type: GameType,
    bat_first_side: Side,
    sky: Sky,
    field_condition: FieldCondition,
    precipitation: Precipitation,
    wind_direction: WindDirection,
    how_scored: HowScored,
    game_file_status: GameFileStatus,
    season: Season,
    park: Park,
    temperature_fahrenheit: Option<u8>,
    attendance: Option<u8>,
    wind_speed_mph: Option<u8>,
    use_dh: bool
}


#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct GameUmpire {
    position: UmpirePosition,
    umpire: Option<Person>
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
struct GameResults {
    winning_pitcher: Option<Person>,
    losing_pitcher: Option<Person>,
    save_pitcher: Option<Person>,
    game_winning_rbi: Option<Person>,
    time_of_game_minutes: u16,
    protest_info: Option<String>,
    completion_info: Option<String>,
    line_score: Option<Matchup<Vec<u8>>>,
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct GameLineupAppearance {
    player: Person,
    lineup_position: LineupPosition,
    start_event: Event,
    end_event: Event
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct GameFieldingAppearance {
    player: Person,
    fielding_position: FieldingPosition,
    lineup_appearance: GameLineupAppearance,
    start_event: Event,
    end_event: Event
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
struct GameContext {
    teams: Matchup<Franchise>,
    date: NaiveDate,
    setting: GameSetting,
    umpires: Vec<GameUmpire>,
    results: GameResults,
    lineup_appearances: Vec<GameLineupAppearance>,
    fielding_appearances: Vec<GameFieldingAppearance>,
    events: Vec<Event>
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct EventStartingBaseState {
    event: Event,
    base: Base,
    runner: LineupPosition,
    charged_to_pitcher: GameFieldingAppearance,
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct EventPlayAtBase {
    event: Event,
    baserunning_play_type: BaserunningPlayType,
    base: Base
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct EventPlateAppearance {
    batter_hand: Handedness,
    pitcher_hand: Handedness,
    plate_appearance_type: PlateAppearanceType,
    hit_location: Option<HitLocation>
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct EventPitchDetail {
    pitch_type: Pitch,
    runners_going: bool,
    blocked_by_catcher: bool,
    catcher_pickoff_attempt: Option<Base>
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct EventFielder {
    fielder: FieldingPosition,
    fielding_play_type: FieldingPlayType,
    exceptional_flag: bool,
    uncertain_flag: bool
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct EventBaserunningAdvances {
    baserunner: BaseRunner,
    attempted_advance_to: Base,
    is_out: bool,
    rbi: bool,
    team_unearned: bool
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct EventContext {
    inning: u8,
    batting_side: Side,
    frame: InningFrame,
    at_bat: LineupPosition,
    outs: u8,
    starting_base_state: Vec<EventStartingBaseState>
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct EventResults {
    count_at_event: Count,
    pitch_sequence: Vec<EventPitchDetail>,
    plate_appearance: Option<EventPlateAppearance>,
    plays_at_base: Vec<EventPlayAtBase>,
    fielding_sequence: Vec<EventFielder>,
    baserunning_advances: Vec<EventBaserunningAdvances>,
    play_info: Vec<EventInfoType>,
    comment: Option<String>
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct Event {
    context: EventContext,
    results: EventResults
}