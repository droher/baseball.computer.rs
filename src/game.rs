use num_traits::cast::FromPrimitive;
use strum_macros::{EnumDiscriminants, EnumString};

use crate::event_file_entities::{Player, Pitcher, Fielder};

#[derive(Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "lowercase")]
enum InningFrame {
    Top,
    Bottom,
}

#[derive(Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "lowercase")]
enum Base {
    First = 1,
    Second,
    Third,
    Home
}

#[derive(Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "camel_case")]
enum PitchType {
    PickoffFirst,
    PickoffSecond,
    PickoffThird,
    PlayNotInvolvingBatter,
    Ball,
    CalledStrike,
    Foul,
    HitBatter,
    IntentionalBall,
    StrikeUnknownType,
    FoulBunt,
    MissedBunt,
    NoPitch,
    FoulTipBunt,
    Pitchout,
    SwingingOnPitchout,
    FoulOnPitchout,
    SwingingStrike,
    FoulTip,
    Unknown,
    BallOnPitcherGoingToMouth,
    InPlay,
    InPlayOnPitchout
}

struct Pitch {
    pitch_type: PitchType,
    runners_going: bool,
    catcher_pickoff_attempt: bool,
    blocked_by_catcher: bool
}

#[derive(Debug, EnumDiscriminants)]
#[strum_discriminants(derive(EnumString))]
enum BatterPlay {
    FlyOut(Fielder),
    GroundOut(Vec<Fielder>),
    DoublePlay(Fielder),
    CatcherInterference,
    OtherFielderInterference(Fielder),
    Single(Fielder),
    Double(Fielder),
    Triple(Fielder),
    GroundRuleDouble,
    ReachedOnError(Fielder),
    FieldersChoice(Fielder),
    ErrorOnFlyBall(Fielder),
    HomeRun,
    InsideTheParkHomeRun(Fielder),
    HitByPitch,
    StrikeOut(Option<RunnerPlay>),
    NoPlay,
    IntentionalWalk(Option<RunnerPlay>),
    Walk(Option<RunnerPlay>)
}

#[derive(Debug, EnumDiscriminants)]
#[strum_discriminants(derive(EnumString))]
enum RunnerPlay {
    Balk,
    CaughtStealing(Base, Vec<Fielder>),
    DefensiveIndifference,
    OtherAdvance,
    PassedBall,
    WildPitch,
    PickedOff(Base, Vec<Fielder>),
    PickedOffCaughtStealing(Base, Vec<Fielder>),
    StolenBase(Base)
}

struct RunnerAdvance {
    from: Base,
    to: Base,

}

type SuccessfulRunnerAdvance = RunnerAdvance;

struct UnsuccessfulRunnerAdvance {
    attempt: RunnerAdvance,
    fielders: Vec<Fielder>
}


type HitLocation = String;

#[derive(Debug, EnumDiscriminants)]
#[strum_discriminants(derive(EnumString))]
enum PlayModifier {
    AppealPlay,
    PopUpBunt(Option<HitLocation>),
    GroundBallBunt(Option<HitLocation>),
    BuntGroundIntoDoublePlay(Option<HitLocation>),
    BatterInterference(Option<HitLocation>),
    LineDriveBunt(Option<HitLocation>),
    BatingOutOfTurn,
    BuntPopUp(Option<HitLocation>),
    BuntPoppedIntoDoublePlay(Option<HitLocation>),
    RunnerHitByBattedBall(Option<HitLocation>),
    CalledThirdStrike,
    CourtesyBatter,
    CourtesyFielder,
    CourtesyRunner,
    UnspecifiedDoublePlay(Option<HitLocation>),
    ErrorOn(Position),
    Fly(Option<HitLocation>),
    FlyBallDoublePlay(Option<HitLocation>),
    FanInterference,
    Foul(Option<HitLocation>),
    ForceOut(Option<HitLocation>),
    GroundBall(Option<HitLocation>),
    GroundBallDoublePlay(Option<HitLocation>),
    GroundBallTriplePlay(Option<HitLocation>),
    InfieldFlyRule(Option<HitLocation>),
    Interference(Option<HitLocation>),
    InsideTheParkHomeRun(Option<HitLocation>),
    LineDrive(Option<HitLocation>),
    LinedIntoDoublePlay(Option<HitLocation>),
    LinedIntoTriplePlay(Option<HitLocation>),
    ManageChallengeOfCallOnField,
    NoDoublePlayCredited,
    Obstruction,
    PopFly(Option<HitLocation>),
    RunnerOutPassingAnotherRunner,
    RelayToFielderWithNoOutMade(Position),
    RunnerInterference,
    SacrificeFly(Option<HitLocation>),
    SacrificeHit(Option<HitLocation>),
    Throw,
    ThrowToBase(Base),
    UnspecifiedTriplePlay(Option<HitLocation>),
    UmpireInterference(Option<HitLocation>),
    UmpireReviewOfCallOnField
}

type Catcher = Player;
type FirstBaseman = Player;
type SecondBaseman = Player;
type ThirdBaseman = Player;
type Shortstop = Player;
type LeftFielder = Player;
type CenterFielder = Player;
type RightFielder = Player;
type DesignatedHitter = Player;
type PinchHitter = Player;
type PinchRunner = Player;

#[derive(Debug)]
pub enum Position {
    Pitcher(Pitcher),
    Catcher(Catcher),
    FirstBaseman(FirstBaseman),
    SecondBaseman(SecondBaseman),
    ThirdBaseman(ThirdBaseman),
    Shortstop(Shortstop),
    LeftFielder(LeftFielder),
    CenterFielder(CenterFielder),
    RightFielder(RightFielder),
    DesignatedHitter(DesignatedHitter),
    PinchHitter(PinchHitter),
    PinchRunner(PinchRunner)
}

