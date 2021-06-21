use std::collections::HashMap;
use std::convert::TryFrom;

use anyhow::{anyhow, bail, Context, Error, Result};
use bimap::BiMap;
use chrono::{NaiveDate, NaiveTime};
use either::Either;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tinystr::TinyStr8;

use crate::event_file::info::{DayNight, DoubleheaderStatus, FieldCondition, HowScored, InfoRecord, Precipitation, Sky, UmpireAssignment, UmpirePosition, WindDirection};
use crate::event_file::misc::{BatHandAdjustment, Comment, LineupAdjustment, PitchHandAdjustment, RunnerAdjustment, SubstitutionRecord};
use crate::event_file::parser::{MappedRecord, RecordVec};
use crate::event_file::pitch_sequence::PitchSequenceItem;
use crate::event_file::play::{Base, BaseRunner, BaserunningPlayType, CachedPlay, ContactDescription, ContactType, Count, EarnedRunStatus, FieldersData, FieldingData, HitLocation, HitType, ImplicitPlayResults, InningFrame, OtherPlateAppearance, OutAtBatType, PlateAppearanceType, Play, PlayRecord, PlayType, RunnerAdvance, RunnerAdvanceModifier};
use crate::event_file::traits::{Inning, Matchup, Pitcher, Player};
use crate::event_file::traits::{FieldingPosition, GameFileStatus, GameType, Handedness, LineupPosition, Side};

const UNKNOWN_STRINGS: [&str;1] = ["unknown"];
const NONE_STRINGS: [&str;2] = ["(none)", "none"];

type Position = Either<LineupPosition, FieldingPosition>;
type PersonnelState = BiMap<Position, Player>;
type Lineup = PersonnelState;
type Defense = PersonnelState;

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum EnteredGameAs {
    Starter,
    PinchHitter,
    PinchRunner,
    DefensiveSubstitution
}

impl EnteredGameAs {
    fn get_substitution_type(sub: &SubstitutionRecord) -> Self {
        match sub.fielding_position {
            FieldingPosition::PinchHitter => Self::PinchHitter,
            FieldingPosition::PinchRunner => Self::PinchRunner,
            _ => Self::DefensiveSubstitution
        }
    }
}

impl TryFrom<&MappedRecord> for EnteredGameAs {
    type Error = Error;

    fn try_from(record: &MappedRecord) -> Result<Self> {
        match record {
            MappedRecord::Start(_) => Ok(Self::Starter),
            MappedRecord::Substitution(sr) => Ok(Self::get_substitution_type(sr)),
            _ => bail!("Appearance type can only be determined from an appearance record")
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum PlateAppearanceResultType {
    Single,
    Double,
    GroundRuleDouble,
    Triple,
    HomeRun,
    InPlayOut,
    StrikeOut,
    FieldersChoice,
    ReachedOnError,
    Interference,
    HitByPitch,
    Walk,
    IntentionalWalk
}

impl From<&PlateAppearanceType> for PlateAppearanceResultType {
    fn from(plate_appearance: &PlateAppearanceType) -> Self {
        match plate_appearance {
            PlateAppearanceType::Hit(h) => match h.hit_type {
                HitType::Single => Self::Single,
                HitType::Double => Self::Double,
                HitType::GroundRuleDouble => Self::GroundRuleDouble,
                HitType::Triple => Self::Triple,
                HitType::HomeRun => Self::HomeRun
            },
            PlateAppearanceType::OtherPlateAppearance(opa ) => match opa {
                OtherPlateAppearance::Walk => Self::Walk,
                OtherPlateAppearance::IntentionalWalk => Self::IntentionalWalk,
                OtherPlateAppearance::HitByPitch => Self::HitByPitch,
                OtherPlateAppearance::Interference => Self::Interference
            },
            PlateAppearanceType::BattingOut(bo) => match bo.out_type {
                OutAtBatType::ReachedOnError => Self::ReachedOnError,
                OutAtBatType::InPlayOut if bo.implicit_advance().is_some() => Self::ReachedOnError,
                OutAtBatType::StrikeOut => Self::StrikeOut,
                OutAtBatType::FieldersChoice => Self::FieldersChoice,
                OutAtBatType::InPlayOut => Self::InPlayOut,
            }
        }
    }
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize)]
enum EventInfoType {}

impl EventInfoType {
    fn from_play(_play: &CachedPlay) -> Vec<Self> {
        vec![]
    }
    }

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
struct Season(u16);

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct League(String);

// #[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize)]
// struct Franchise {
//     retrosheet_id: String,
//     franchise_name: String
// }
//
// #[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize)]
// struct Division {
//     league: League,
//     division_name: String
// }
//
// #[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize)]
// struct City {
//     city_name: Option<String>,
//     state_name: Option<String>,
//     country_name: String
// }

// #[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize)]
// struct Person {
//     date_of_birth: Option<NaiveDate>,
//     date_of_death: Option<NaiveDate>,
//     bats: Handedness,
//     throws: Handedness,
//     // Provide default here if birth_date is populated
//     birth_year: Option<u16>,
//     weight_pounds: Option<u16>,
//     height_inches: Option<u16>,
//     place_of_birth: City,
//     place_of_death: City,
//     retrosheet_id: String,
//     full_name: String
// }

// #[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize)]
// struct Park {
//     park_name: String,
//     alias: String,
//     retrosheet_id: String,
//     city: City
// }

// #[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize)]
// struct FranchiseSeason {
//     franchise: Franchise,
//     season: Season,
//     division: Division
// }

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Serialize, Deserialize,)]
struct GameSetting {
    date: NaiveDate,
    start_time: Option<NaiveTime>,
    doubleheader_status: DoubleheaderStatus,
    time_of_day: DayNight,
    game_type: GameType,
    bat_first_side: Side,
    sky: Sky,
    field_condition:  FieldCondition,
    precipitation: Precipitation,
    wind_direction: WindDirection,
    how_scored: HowScored,
    game_file_status: GameFileStatus,
    season: Season,
    park_id: String,
    temperature_fahrenheit: Option<u8>,
    attendance: Option<u32>,
    wind_speed_mph: Option<u8>,
    use_dh: bool
}

impl Default for GameSetting {
    fn default() -> Self {
        Self {
            date: NaiveDate::from_num_days_from_ce(0),
            doubleheader_status: Default::default(),
            start_time: Default::default(),
            time_of_day: Default::default(),
            use_dh: false,
            bat_first_side: Side::Away,
            sky: Default::default(),
            temperature_fahrenheit: Default::default(),
            field_condition: Default::default(),
            precipitation: Default::default(),
            wind_direction: Default::default(),
            how_scored: Default::default(),
            game_file_status: GameFileStatus::Event,
            wind_speed_mph: Default::default(),
            attendance: None,
            park_id: Default::default(),
            game_type: GameType::RegularSeason,
            season: Season(0)
        }
    }
}

impl From<&RecordVec> for GameSetting {
    fn from(vec: &RecordVec) -> Self {

        let infos = vec.iter()
            .filter_map(|rv| if let MappedRecord::Info(i) = rv {Some(i)} else {None});

        let mut setting = Self::default();

        for info in infos {

            match info {
                InfoRecord::GameDate(x) => {setting.date = *x},
                InfoRecord::DoubleheaderStatus(x) => {setting.doubleheader_status = *x},
                InfoRecord::StartTime(x) => {setting.start_time = *x },
                InfoRecord::DayNight(x) => {setting.time_of_day = *x},
                InfoRecord::UseDh(x) => {setting.use_dh = *x},
                InfoRecord::HomeTeamBatsFirst(x) => {
                    setting.bat_first_side = if *x {Side::Home} else {Side::Away}
                },
                InfoRecord::Sky(x) => {setting.sky = *x},
                InfoRecord::Temp(x) => {setting.temperature_fahrenheit = *x},
                InfoRecord::FieldCondition(x) => {setting.field_condition = *x}
                InfoRecord::Precipitation(x) => {setting.precipitation = *x}
                InfoRecord::WindDirection(x) => {setting.wind_direction = *x},
                InfoRecord::WindSpeed(x) => {setting.wind_speed_mph = *x},
                InfoRecord::Attendance(x) => {setting.attendance = *x },
                InfoRecord::Park(x) => {setting.park_id = x.to_string()},
                InfoRecord::HowScored(x) => {setting.how_scored = *x},
                // TODO: Season, GameType
                _ => {}
            }
        }
        setting
    }
}


#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct GameUmpire {
    position: UmpirePosition,
    umpire: Option<String>
}


impl GameUmpire {
    // Retrosheet has two different possible null-like values for umpire names, "none"
    // and "unknown". We take "none" to mean that there was no umpire at that position,
    // so we do not create a record. If "unknown", we assume there was someone at that position,
    // so a struct is created with a None umpire ID.
    fn from_umpire_assignment(ua: &UmpireAssignment) -> Option<Self> {
        let umpire = ua.umpire?;
        let position = ua.position;
        if NONE_STRINGS.contains(&umpire.as_str()) { None }
        else if UNKNOWN_STRINGS.contains(&umpire.as_str()) {
            Some(Self {position, umpire: None})
        }
        else {
            Some(Self {position, umpire: Some(umpire.to_string())})
        }
    }

    fn from_record_vec(vec: &RecordVec) -> Vec<Self> {
        vec.iter()
            .filter_map(|rv| if let MappedRecord::Info(InfoRecord::UmpireAssignment(ua)) = rv
                { Self::from_umpire_assignment(ua) } else { None }
            )
            .collect_vec()
    }
}


#[derive(Debug, Eq, PartialEq, Clone, Serialize, Default)]
struct GameResults {
    winning_pitcher: Option<String>,
    losing_pitcher: Option<String>,
    save_pitcher: Option<String>,
    game_winning_rbi: Option<String>,
    time_of_game_minutes: Option<u16>,
    protest_info: Option<String>,
    completion_info: Option<String>,
    //line_score: Matchup<LineScore>
}

impl From<&Vec<MappedRecord>> for GameResults {

    fn from(vec: &Vec<MappedRecord>) -> Self {
        let s = |opt_str: &Option<TinyStr8>| opt_str.map(|t| t.to_string());

        let mut results = Self::default();
        vec.iter()
            .filter_map(|rv| if let MappedRecord::Info(i) = rv {Some(i)} else { None })
            .for_each(|info| match info {
                InfoRecord::WinningPitcher(x) => {results.winning_pitcher = s(x)},
                InfoRecord::LosingPitcher(x) => {results.losing_pitcher = s(x)},
                InfoRecord::SavePitcher(x) => {results.save_pitcher = s(x)},
                InfoRecord::GameWinningRbi(x) => {results.game_winning_rbi = s(x)},
                InfoRecord::TimeOfGameMinutes(x) => {results.time_of_game_minutes = *x},
                _ => {}
            });
        results
    }
}


#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
pub struct GameLineupAppearance {
    player: String,
    lineup_position: LineupPosition,
    entered_game_as: EnteredGameAs,
    start_event: u16,
    end_event: Option<u16>
}

impl GameLineupAppearance {
    fn new_starter(player: String, lineup_position: LineupPosition) -> Self {
        Self {
            player,
            lineup_position,
            entered_game_as: EnteredGameAs::Starter,
            start_event: 1,
            end_event: None
        }
    }
}

#[derive(Default, Debug, Eq, PartialEq, Clone, Serialize)]
pub struct GameFieldingAppearance {
    player: String,
    fielding_position: FieldingPosition,
    start_event: u16,
    end_event: Option<u16>
}

impl GameFieldingAppearance {
    fn new_starter(player: String, fielding_position: FieldingPosition) -> Self {
        Self {
            player,
            fielding_position,
            start_event: 1,
            end_event: None
        }
    }

    fn new(player: String, fielding_position: FieldingPosition, start_event: u16) -> Self {
        Self {
            player,
            fielding_position,
            start_event,
            end_event: None
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
pub struct GameContext {
    teams: Matchup<String>,
    setting: GameSetting,
    umpires: Vec<GameUmpire>,
    results: GameResults,
    lineup_appearances: Vec<GameLineupAppearance>,
    fielding_appearances: Vec<GameFieldingAppearance>,
    events: Vec<Event>
}

impl TryFrom<&RecordVec> for GameContext {
    type Error = Error;

    fn try_from(record_vec: &RecordVec) -> Result<Self> {
        let teams: Matchup<String> = Matchup::try_from(record_vec)?
            .apply_both(|t| t.to_string())
            .into();
        let setting = GameSetting::try_from(record_vec)?;
        let umpires = GameUmpire::from_record_vec(record_vec);
        let results = GameResults::try_from(record_vec)?;

        let (events, lineup_appearances, fielding_appearances) = GameState::create_events(record_vec)?;
        Ok(Self {
            teams,
            setting,
            umpires,
            results,
            lineup_appearances,
            fielding_appearances,
            events
        })
    }

}

#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
struct EventStartingBaseState {
    baserunner: BaseRunner,
    runner_lineup_position: LineupPosition,
    charged_to_pitcher: String,
}

impl EventStartingBaseState {
    fn from_base_state(state: &BaseState) -> Vec<Self> {
        state.get_bases()
            .iter()
            .map(|(baserunner, runner)| Self {
                baserunner: *baserunner,
                runner_lineup_position: runner.lineup_position,
                charged_to_pitcher: runner.charged_to.to_string()
            })
            .collect_vec()
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct EventBaserunningPlay {
    baserunning_play_type: BaserunningPlayType,
    base: Option<Base>
}

impl EventBaserunningPlay {
    fn from_play(play: &CachedPlay) -> Option<Vec<Self>> {
        let vec = play.play
            .main_plays
            .iter()
            .filter_map(|pt| if let PlayType::BaserunningPlay(br) = pt {
                Some(Self {baserunning_play_type: br.baserunning_play_type, base: br.at_base})
            } else {None})
            .collect_vec();
        if vec.is_empty() { None } else { Some(vec) }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct EventPlateAppearance {
    batter_hand: Handedness,
    pitcher_hand: Handedness,
    plate_appearance_type: PlateAppearanceResultType,
    contact_type: Option<ContactType>,
    hit_location: Option<HitLocation>
}

impl EventPlateAppearance {
    fn from_play(play: &CachedPlay) -> Option<Self> {
        play.play
            .main_plays
            .iter()
            .find_map(|pt| if let PlayType::PlateAppearance(pa) = pt {
                Some(Self {
                    batter_hand: Handedness::Unknown,
                    pitcher_hand: Handedness::Unknown,
                    plate_appearance_type: PlateAppearanceResultType::from(pa),
                    contact_type: play.contact_description.map(|cd| cd.contact_type),
                    hit_location: play.contact_description.map(|cd| cd.location).flatten(),
                })
            } else {None})
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct EventBaserunningAdvances {
    baserunner: BaseRunner,
    attempted_advance_to: Base,
    is_out: bool,
    advanced_on_error: bool,
    rbi: bool,
    team_unearned: bool
}

impl EventBaserunningAdvances {
    fn from_play(play: &CachedPlay) -> Vec<Self> {
        play.advances
            .iter()
            .map(|ra| Self {
                baserunner: ra.baserunner,
                attempted_advance_to: ra.to,
                advanced_on_error: FieldersData::find_error(ra.fielders_data().as_slice()).is_some(),
                is_out: ra.is_out(),
                rbi: play.rbi.contains(&ra.baserunner),
                team_unearned: ra.earned_run_status() == Some(EarnedRunStatus::TeamUnearned)
            })
            .collect_vec()
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
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
    pitch_sequence: Option<Vec<PitchSequenceItem>>,
    plate_appearance: Option<EventPlateAppearance>,
    plays_at_base: Option<Vec<EventBaserunningPlay>>,
    fielding_plays: Vec<FieldersData>,
    baserunning_advances: Vec<EventBaserunningAdvances>,
    play_info: Vec<EventInfoType>,
    comment: Option<String>
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
pub struct Event {
    context: EventContext,
    results: EventResults
}

/// TODO: This tracks the more unusual/miscellaneous elements of the state of the game,,
///     such as batters batting from an unexpected side or a substitution in the middle of
///     an at-bat. Further exceptions should go here as they come up.
#[derive(Default, Debug, Eq, PartialEq, Clone)]
pub struct WeirdGameState {
    batter_hand: Option<Handedness>,
    pitcher_hand: Option<Handedness>,
    responsible_batter: Option<String>,
    responsible_pitcher: Option<String>,
    mid_at_bat_interruption_flag: bool
}

/// Keeps track of the current players on the field at any given point
/// and records their exits/entries.
#[derive(Debug, Eq, PartialEq, Clone)]
struct Personnel {
    personnel_state: Matchup<(Lineup, Defense)>,
    // A player should only have one lineup position per game,
    // but can move freely from one defensive position to another.
    // However, in the rare case of a courtesy runner, a player can
    // potentially become a pinch-runner for another player before
    // switching back to his old lineup position.
    // (This also makes the convenient assumption that a player cannot play for both sides in
    // the same game, which has never happened but could theoretically).
    lineup_appearances: HashMap<Player, Vec<GameLineupAppearance>>,
    defense_appearances: HashMap<Player, Vec<GameFieldingAppearance>>,
}

impl Default for Personnel {
    fn default() -> Self {
        Self {
            personnel_state: Matchup::new((BiMap::with_capacity(15), BiMap::with_capacity(15)),
                                          (BiMap::with_capacity(15), BiMap::with_capacity(15))),
            lineup_appearances: HashMap::with_capacity(30),
            defense_appearances: HashMap::with_capacity(30)
        }
    }
}

impl Personnel {

    fn new(record_vec: &RecordVec) -> Self {
        let mut personnel = Self::default();
        let start_iter = record_vec.iter()
            .filter_map(|rv|
                if let MappedRecord::Start(sr) = rv {Some(sr)} else {None});
        for start in start_iter {
            let (lineup, defense) = personnel.personnel_state.get_mut(&start.side);
            let lineup_appearance = GameLineupAppearance::new_starter(
                start.player.to_string(), start.lineup_position
            );
            let fielding_appearance = GameFieldingAppearance::new_starter(
                start.player.to_string(), start.fielding_position
            );
            lineup.insert(Either::Left(start.lineup_position), start.player);
            defense.insert(Either::Right(start.fielding_position), start.player);
            personnel.lineup_appearances.insert(start.player, vec![lineup_appearance]);
            personnel.defense_appearances.insert(start.player, vec![fielding_appearance]);

        }
        personnel
    }

    fn pitcher(&self, side: &Side) -> Result<Pitcher> {
        self.get_at_position(side, &Either::Right(FieldingPosition::Pitcher))
    }

    fn get_at_position(&self, side: &Side, position: &Position) -> Result<Player> {
        let map_tup = self.personnel_state.get(side);
        let map = if let Either::Left(_) = position {&map_tup.0} else {&map_tup.1};
        map.get_by_left(position)
            .copied()
            .with_context(|| format!("Position {:?} for side {:?} missing from current game state: {:?}", position, side, map))
    }

    fn at_bat(&self, play: &CachedPlay) -> Result<LineupPosition> {
        let position = self.personnel_state
            .get(&play.batting_side)
            .0
            .get_by_right(&play.batter)
            .copied();

        if let Some(Either::Left(lp)) = position {
            Ok(lp)
        } else {bail!("Cannot find lineup position of player currently at bat {:?}.\nFull state: {:?}", &play.batter, self.personnel_state)}
    }

    fn get_current_lineup_appearance(&mut self, player: &Player) -> Result<&mut GameLineupAppearance> {
        self.lineup_appearances
            .get_mut(player)
            .with_context(|| format!("Cannot find existing player {:?} in appearance records", player))?
            .last_mut()
            .with_context(|| format!("Player {:?} has an empty list of lineup appearances", player))
    }

    fn get_current_fielding_appearance(&mut self, player: &Player) -> Result<&mut GameFieldingAppearance> {
        self.defense_appearances
            .get_mut(player)
            .with_context(|| format!("Cannot find existing player {:?} in appearance records", player))?
            .last_mut()
            .with_context(|| format!("Player {:?} has an empty list of fielding appearances", player))
    }

    fn update_lineup_on_substitution(&mut self, sub: &SubstitutionRecord, sequence: u16) -> Result<()> {
        // There should almost always be an original batter, but in
        // the extremely rare case of a courtesy runner, there might not be.
        let original_batter = self.get_at_position(&sub.side, &Either::Left(sub.lineup_position)).ok();
        if let Some(p) = original_batter {
            let original_lineup_appearance = self.get_current_lineup_appearance(&p)?;
            original_lineup_appearance.end_event = Some(sequence);
        }

        let (lineup, _) = self.personnel_state.get_mut(&sub.side);

        let new_lineup_appearance = GameLineupAppearance {
            player: sub.player.to_string(),
            lineup_position: sub.lineup_position,
            entered_game_as: EnteredGameAs::Starter,
            start_event: sequence,
            end_event: None
        };
        lineup.insert(Either::Left(sub.lineup_position), sub.player);
        self.lineup_appearances
            .entry(sub.player)
            .or_insert_with(|| Vec::with_capacity(1))
            .push(new_lineup_appearance);
        Ok(())
    }

    // The semantics of defensive substitutions are more complicated, because the new player
    // could already have been in the game, and the replaced player might not have left the game.
    fn update_defense_on_substitution(&mut self, sub: &SubstitutionRecord, sequence: u16) -> Result<()> {
        let original_fielder = self.get_at_position(&sub.side, &Either::Right(sub.fielding_position)).ok();
        if let Some(p) = original_fielder {
            self.get_current_fielding_appearance(&p)?.end_event = Some(sequence);
        }
        let (_, defense) = self.personnel_state.get_mut(&sub.side);

        // We maintain a 1:1 relationship between players and positions at all times,
        // so the entire position must be removed from the defense temporarily.
        // If the data is consistent, this state (< 9 positions) can only exist between substitutions,
        // and cannot exist at the start of a play.
        defense.remove_by_right(&sub.player);
        defense.insert( Either::Right(sub.fielding_position), sub.player);

        self.defense_appearances.entry(sub.player)
            .or_insert_with(|| Vec::with_capacity(1))
            .push(GameFieldingAppearance::new(sub.player.to_string(),
                                              sub.fielding_position,
                                              sequence));

        Ok(())
    }

    fn update_on_substitution(&mut self, sub: &SubstitutionRecord, sequence: u16) -> Result<()> {
        if sub.lineup_position.bats_in_lineup() { self.update_lineup_on_substitution(sub, sequence)? };
        if sub.fielding_position.plays_in_field() { self.update_defense_on_substitution(sub, sequence)? };

        Ok(())

    }

}

struct HandednessPair {
    batter: Option<Handedness>,
    pitcher: Option<Handedness>
}

/// Tracks the information necessary to populate each event.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct GameState {
    inning: Inning,
    frame: InningFrame,
    batting_side: Side,
    outs: Outs,
    bases: BaseState,
    at_bat: LineupPosition,
    personnel: Personnel,
}

impl GameState {

    pub fn create_events(record_vec: &RecordVec) -> Result<(Vec<Event>, Vec<GameLineupAppearance>, Vec<GameFieldingAppearance>)> {
        let mut events: Vec<Event> = Vec::with_capacity(100);

        let mut sequence: u16 = 1;
        let mut state = Self::new(record_vec);
        for record in record_vec {
            let (pr, cached_play) = if let MappedRecord::Play(pr) = record {
                (Some(pr), Some(CachedPlay::try_from(pr)?))
            } else { (None, None) };
            state.update(record, sequence, &cached_play)?;
            if let (Some(pr), Some(play)) = (pr, cached_play) {
                let context = EventContext {
                    inning: state.inning,
                    batting_side: state.batting_side,
                    frame: state.frame,
                    at_bat: state.at_bat,
                    outs: state.outs,
                    starting_base_state: EventStartingBaseState::from_base_state(&state.bases)
                };
                let results = EventResults {
                    count_at_event: pr.count,
                    pitch_sequence: pr.pitch_sequence.as_ref().map(|ps| ps.0.clone()),
                    plate_appearance: EventPlateAppearance::from_play(&play),
                    plays_at_base: EventBaserunningPlay::from_play(&play),
                    baserunning_advances: EventBaserunningAdvances::from_play(&play),
                    play_info: EventInfoType::from_play(&play),
                    comment: None,
                    fielding_plays: play.fielders_data.clone(),
                };
                events.push(Event {context, results} );
                sequence += 1;
            }
        }
        Ok((events,
            state
                .personnel
                .lineup_appearances
                .values()
                .flatten()
                .cloned()
                .collect_vec(),
            state
                .personnel
                .defense_appearances
                .values()
                .flatten()
                .cloned()
                .collect_vec()))
    }

    pub(crate) fn new(record_vec: &RecordVec) -> Self {
        let batting_side = record_vec.iter()
            .find_map(|rv|
                            if let MappedRecord::Info(InfoRecord::HomeTeamBatsFirst(b)) = rv
                            { Some(if *b {Side::Home} else {Side::Away}) } else {None})
            .map_or(Side::Away, |s| s);

        Self {
            inning: 1,
            frame: InningFrame::Top,
            batting_side,
            outs: 0,
            bases: Default::default(),
            at_bat: Default::default(),
            personnel: Personnel::new(record_vec)
        }
    }

    fn is_frame_flipped(&self, play: &CachedPlay) -> Result<bool> {
        if self.batting_side != play.batting_side {
            if self.outs < 3 { bail!("New frame without 3 outs recorded") }
            else { Ok(true) }
        } else { Ok(false) }
    }

    fn get_new_frame(&self, play: &CachedPlay) -> Result<InningFrame> {
        Ok(if self.is_frame_flipped(play)? {self.frame.flip()} else {self.frame})
    }

    fn outs_after_play(&self, play: &CachedPlay) -> Result<u8> {
        let play_outs = play.outs.len() as u8;
        match if self.is_frame_flipped(play)? {play_outs} else {self.outs + play_outs} {
            o if o > 3 => bail!("Illegal state, more than 3 outs recorded"),
            o => Ok(o)
        }
    }

    fn update_on_play(&mut self, play: &CachedPlay) -> Result<()> {
        if play.play.no_play() {return Ok(())}
        let new_frame = self.get_new_frame(&play)?;
        let new_outs = self.outs_after_play(&play)?;

        let pitcher = self.personnel.pitcher(&play.batting_side.flip())?;
        let batter_lineup_position = self.personnel.at_bat(&play)?;

        let new_base_state = self.bases.new_base_state(
            self.is_frame_flipped(&play)?,
            new_outs == 3,
            &play,
            batter_lineup_position,
            pitcher
        )?;

        self.inning = play.inning;
        self.frame = new_frame;
        self.batting_side = play.batting_side;
        self.outs = new_outs;
        self.bases = new_base_state;
        self.at_bat = batter_lineup_position;

        Ok(())

    }

    fn update_on_substitution(&mut self, record: &SubstitutionRecord, sequence: u16) -> Result<()> {
        self.personnel.update_on_substitution(record, sequence)
    }

    fn update_on_bat_hand_adjustment(&mut self, _record: &BatHandAdjustment) {
        // TODO
    }

    fn update_on_pitch_hand_adjustment(&mut self, _record: &PitchHandAdjustment) {
        // TODO
    }

    fn update_on_lineup_adjustment(&mut self, _record: &LineupAdjustment) {
        // Nothing to do here, since we map player to batting order anyway
    }

    fn update_on_runner_adjustment(&mut self, record: &RunnerAdjustment) -> Result<()> {
        // The 2020 tiebreaker runner record can appear before or after the first record of the next
        // inning, and it doesn't have a side associated with it, so we have to do some messy
        // state changes to get it right.
        if self.outs == 3 {
            self.frame = self.frame.flip();
            self.batting_side = self.batting_side.flip();
            self.outs = 0;
        }

        let runner_pos = self.personnel
            .get_current_lineup_appearance(&record.runner_id)?
            .lineup_position;
        let pitcher = self.personnel.pitcher(&self.batting_side.flip())?;
        self.bases = BaseState::new_inning_tiebreaker(runner_pos, pitcher);

        Ok(())
    }

    fn update_on_comment(&mut self, _record: &Comment) {
        // TODO
    }

    pub fn update(&mut self, record: &MappedRecord, sequence: u16, cached_play: &Option<CachedPlay>) -> Result<()> {
        Ok(match record {
            MappedRecord::Play(_) => {
                if let Some(cp) = cached_play
                { self.update_on_play(cp) } else { bail!("Expected cached play but got None") }
            }?,
            MappedRecord::Substitution(r) => self.update_on_substitution(r, sequence)?,
            MappedRecord::BatHandAdjustment(r) => self.update_on_bat_hand_adjustment(r),
            MappedRecord::PitchHandAdjustment(r) => self.update_on_pitch_hand_adjustment(r),
            MappedRecord::LineupAdjustment(r) => self.update_on_lineup_adjustment(r),
            MappedRecord::RunnerAdjustment(r) => self.update_on_runner_adjustment(r)?,
            MappedRecord::Comment(r) => self.update_on_comment(r),
            _ => ()
        })
    }
}

pub type Outs = u8;

#[derive(Debug, Eq, PartialEq, Default, Clone)]
pub struct BaseState {
    bases: HashMap<BaseRunner, Runner>,
    scored: Vec<Runner>
}

impl BaseState {
    pub fn new_inning_tiebreaker(new_runner: LineupPosition, current_pitcher: Pitcher) -> Self {
        let mut state = Self::default();
        let runner = Runner { lineup_position: new_runner, charged_to: current_pitcher };
        state.bases.insert(BaseRunner::Second, runner);
        state
    }

    pub fn get_bases(&self) -> &HashMap<BaseRunner, Runner> {
        &self.bases
    }

    fn num_runners_on_base(&self) -> u8 {
        self.bases.len() as u8
    }

    fn get_runner(&self, baserunner: &BaseRunner) -> Option<&Runner> {
        self.bases.get(baserunner)
    }

    fn get_first(&self) -> Option<&Runner> {
        self.bases.get( &BaseRunner::First)
    }

    fn get_second(&self) -> Option<&Runner> {
        self.bases.get( &BaseRunner::Second)
    }

    fn get_third(&self) -> Option<&Runner> {
        self.bases.get( &BaseRunner::Third)
    }

    fn clear_baserunner(&mut self, baserunner: &BaseRunner) -> Option<Runner> {
        self.bases
            .remove(baserunner)
    }

    fn set_runner(&mut self, baserunner: BaseRunner, runner: Runner) {
        self.bases.insert(baserunner, runner);
    }

    fn get_advance_from_baserunner(baserunner: BaseRunner, cached_play: &CachedPlay) -> Option<&RunnerAdvance> {
        cached_play
            .advances
            .iter()
            .find(|a| a.baserunner == baserunner)
    }

    fn current_base_occupied(&self, advance: &RunnerAdvance) -> bool {
        self.get_runner(&advance.baserunner).is_some()
    }

    fn target_base_occupied(&self, advance: &RunnerAdvance) -> Result<bool> {
        let br = BaseRunner::from_target_base(&advance.to);
        Ok(self.get_runner(&br?).is_some())

    }

    fn check_integrity(old_state: &Self, new_state: &Self, advance: &RunnerAdvance) -> Result<()> {
        if new_state.target_base_occupied(advance)? {
            bail!("Runner is listed as moving to a base that is occupied by another runner")
        }
        else if !old_state.current_base_occupied(advance) {
            bail!("Advancement from a base that had no runner on it.\n\
            Old state: {:?}\n\
            New state: {:?}\n\
            Advance: {:?}\n", old_state, new_state, advance)
        }
        else {
            Ok(())
        }
    }

    ///  Accounts for Rule 9.16(g) regarding the assignment of trailing
    ///  baserunners as inherited if they reach on a fielder's choice
    ///  in which an inherited runner is forced out 🙃
    fn update_runner_charges(self, _play: &Play) -> Self {
        // TODO: This
        self
    }

    pub(crate) fn new_base_state(&self, start_inning: bool, end_inning: bool, cached_play: &CachedPlay, batter_lineup_position: LineupPosition, pitcher: Pitcher) -> Result<Self> {
        let play = &cached_play.play;

        let mut new_state = if start_inning {Self::default()} else {self.clone()};
        new_state.scored = vec![];

        // Cover cases where outs are not included in advance information
        for out in &cached_play.outs {
            new_state.clear_baserunner(out);
        }

        if let Some(a) = BaseState::get_advance_from_baserunner(BaseRunner::Third, &cached_play) {
            new_state.clear_baserunner(&BaseRunner::Third);
            if a.is_out() {}
            else if let Err(e) = Self::check_integrity(&self, &new_state, &a) {
                return Err(e)
            }
            else if let Some(r) = self.get_third() {
                new_state.scored.push(*r)
            }
        }
        if let Some(a) = BaseState::get_advance_from_baserunner(BaseRunner::Second, &cached_play) {
            new_state.clear_baserunner(&BaseRunner::Second);
            if a.is_out() {}
            else if let Err(e) = Self::check_integrity(&self, &new_state, &a) {
                return Err(e)
            }
            else if let (Ok(true), Some(r)) = (a.is_this_that_one_time_jean_segura_ran_in_reverse(), self.get_second()) {
                new_state.set_runner(BaseRunner::First, *r)
            }
            else if let (Base::Third, Some(r)) = (a.to, self.get_second()) {
                new_state.set_runner(BaseRunner::Third, *r)
            }
            else if let (Base::Home, Some(r)) = (a.to, self.get_second()) {
                new_state.scored.push(*r)
            }
        }
        if let Some(a) = BaseState::get_advance_from_baserunner(BaseRunner::First, &cached_play) {
            new_state.clear_baserunner(&BaseRunner::First);
            if a.is_out() {}
            else if let Err(e) = Self::check_integrity(&self, &new_state, &a) {
                return Err(e)
            }
            else if let (Base::Second, Some(r)) = (&a.to, self.get_first()) {
                new_state.set_runner(BaseRunner::Second, *r)
            }
            else if let (Base::Third, Some(r)) = (&a.to, self.get_first()) {
                new_state.set_runner(BaseRunner::Third, *r)
            }
            else if let (Base::Home, Some(r)) = (&a.to, self.get_first()) {
                new_state.scored.push(*r)
            }
        }
        if let Some(a) = BaseState::get_advance_from_baserunner(BaseRunner::Batter, &cached_play) {
            let new_runner = Runner { lineup_position: batter_lineup_position, charged_to: pitcher };
            match a.to {
                _ if a.is_out() || end_inning => {},
                _ if new_state.target_base_occupied(&a)? => return Err(anyhow!("Batter advanced to an occupied base")),
                Base::Home => new_state.scored.push(new_runner),
                b => new_state.set_runner(BaseRunner::from_current_base(&b)?, new_runner)
            }
        }
        Ok(new_state.update_runner_charges(&play))
    }


}


#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct Runner {pub lineup_position: LineupPosition, pub charged_to: Pitcher}
