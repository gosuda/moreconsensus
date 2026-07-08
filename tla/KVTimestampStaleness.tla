---- MODULE KVTimestampStaleness ----
EXTENDS Naturals, FiniteSets

VARIABLES selector, queryKey, refStamp, lag, lowerBound, upperBound, targetStamp, pointRow, scanRows

Keys == {"a", "b"}
Values == {"a1", "a3", "b2", "b4"}
StampSet == 0..5
Lags == 0..6
Selectors == {"init", "at", "within", "exact", "bounded", "exact_staleness"}

NoRow == [present |-> FALSE, key |-> "none", value |-> "none", stamp |-> 0]

History == {
    [key |-> "a", value |-> "a1", stamp |-> 1, live |-> TRUE],
    [key |-> "a", value |-> "a3", stamp |-> 3, live |-> TRUE],
    [key |-> "a", value |-> "a3", stamp |-> 5, live |-> FALSE],
    [key |-> "b", value |-> "b2", stamp |-> 2, live |-> TRUE],
    [key |-> "b", value |-> "b4", stamp |-> 4, live |-> TRUE]
}

Vars == <<selector, queryKey, refStamp, lag, lowerBound, upperBound, targetStamp, pointRow, scanRows>>

Row(r) == [present |-> TRUE, key |-> r.key, value |-> r.value, stamp |-> r.stamp]
ScanRow(r) == [key |-> r.key, value |-> r.value, stamp |-> r.stamp]


LiveCandidates(k, lo, hi) ==
    {r \in History : r.live /\ r.key = k /\ lo <= r.stamp /\ r.stamp <= hi}

LatestLive(k, lo, hi) ==
    LET candidates == LiveCandidates(k, lo, hi)
    IN IF candidates = {}
       THEN NoRow
       ELSE Row(CHOOSE r \in candidates : \A other \in candidates : other.stamp <= r.stamp)

ScanRows(lo, hi) ==
    {ScanRow(r) : r \in {h \in History : h.live /\ lo <= h.stamp /\ h.stamp <= hi /\
        \A other \in LiveCandidates(h.key, lo, hi) : other.stamp <= h.stamp}}

TypeOK ==
    /\ selector \in Selectors
    /\ queryKey \in Keys
    /\ refStamp \in StampSet
    /\ lag \in Lags
    /\ lowerBound \in StampSet
    /\ upperBound \in StampSet
    /\ targetStamp \in StampSet
    /\ pointRow \in [present : BOOLEAN, key : Keys \cup {"none"}, value : Values \cup {"none"}, stamp : StampSet]
    /\ scanRows \subseteq [key : Keys, value : Values, stamp : StampSet]

Init ==
    /\ selector = "init"
    /\ queryKey = "a"
    /\ refStamp = 0
    /\ lag = 0
    /\ lowerBound = 0
    /\ upperBound = 0
    /\ targetStamp = 0
    /\ pointRow = NoRow
    /\ scanRows = {}

ReadWindow(nextSelector, k, ref, staleness, lo, hi, target) ==
    /\ selector' = nextSelector
    /\ queryKey' = k
    /\ refStamp' = ref
    /\ lag' = staleness
    /\ lowerBound' = lo
    /\ upperBound' = hi
    /\ targetStamp' = target
    /\ pointRow' = LatestLive(k, lo, hi)
    /\ scanRows' = ScanRows(lo, hi)

ReadAt(k, atStamp) ==
    ReadWindow("at", k, atStamp, 0, 0, atStamp, atStamp)

ReadWithin(k, lo, hi) ==
    /\ lo <= hi
    /\ ReadWindow("within", k, hi, 0, lo, hi, hi)

ReadExact(k, stamp) ==
    ReadWindow("exact", k, stamp, 0, stamp, stamp, stamp)

ReadBounded(k, ref, staleness) ==
    LET lo == IF staleness > ref THEN 0 ELSE ref - staleness
    IN ReadWindow("bounded", k, ref, staleness, lo, ref, ref)

ReadExactStaleness(k, ref, staleness) ==
    IF staleness > ref
    THEN /\ selector' = "exact_staleness"
         /\ queryKey' = k
         /\ refStamp' = ref
         /\ lag' = staleness
         /\ lowerBound' = 1
         /\ upperBound' = 0
         /\ targetStamp' = 0
         /\ pointRow' = NoRow
         /\ scanRows' = {}
    ELSE LET target == ref - staleness
         IN ReadWindow("exact_staleness", k, ref, staleness, target, target, target)

Next ==
    \/ \E k \in Keys, atStamp \in StampSet : ReadAt(k, atStamp)
    \/ \E k \in Keys, lo \in StampSet, hi \in StampSet : ReadWithin(k, lo, hi)
    \/ \E k \in Keys, stamp \in StampSet : ReadExact(k, stamp)
    \/ \E k \in Keys, ref \in StampSet, staleness \in Lags : ReadBounded(k, ref, staleness)
    \/ \E k \in Keys, ref \in StampSet, staleness \in Lags : ReadExactStaleness(k, ref, staleness)

PointReadMatchesWindow ==
    selector # "init" =>
        /\ (pointRow.present <=> LiveCandidates(queryKey, lowerBound, upperBound) # {})
        /\ (pointRow.present => pointRow = LatestLive(queryKey, lowerBound, upperBound))

AtSelectorUsesInclusiveUpperBound ==
    selector = "at" => lowerBound = 0 /\ upperBound = targetStamp

WithinSelectorUsesInclusiveBounds ==
    selector = "within" => lowerBound <= upperBound

ExactSelectorUsesSingleStamp ==
    selector = "exact" => lowerBound = targetStamp /\ upperBound = targetStamp

BoundedSelectorUsesReferenceWindow ==
    selector = "bounded" =>
        /\ upperBound = refStamp
        /\ lowerBound = IF lag > refStamp THEN 0 ELSE refStamp - lag

ExactStalenessSelectorUsesReferenceOffset ==
    selector = "exact_staleness" =>
        IF lag > refStamp
        THEN /\ pointRow = NoRow
             /\ scanRows = {}
             /\ lowerBound > upperBound
        ELSE /\ targetStamp = refStamp - lag
             /\ lowerBound = targetStamp
             /\ upperBound = targetStamp

ScanRowsMatchWindow ==
    selector # "init" => scanRows = ScanRows(lowerBound, upperBound)

ScanRowsCarryMetadata ==
    \A row \in scanRows :
        /\ row.key \in Keys
        /\ row.value \in Values
        /\ row.stamp \in StampSet

ConcreteSelectorCases ==
    /\ (selector = "at" /\ queryKey = "a" /\ upperBound = 2) =>
        pointRow = [present |-> TRUE, key |-> "a", value |-> "a1", stamp |-> 1]
    /\ (selector = "at" /\ queryKey = "a" /\ upperBound = 0) => pointRow = NoRow
    /\ (selector = "within" /\ queryKey = "a" /\ lowerBound = 2 /\ upperBound = 4) =>
        pointRow = [present |-> TRUE, key |-> "a", value |-> "a3", stamp |-> 3]
    /\ (selector = "exact" /\ queryKey = "b" /\ targetStamp = 4) =>
        pointRow = [present |-> TRUE, key |-> "b", value |-> "b4", stamp |-> 4]
    /\ (selector = "exact" /\ queryKey = "b" /\ targetStamp = 3) => pointRow = NoRow
    /\ (selector = "bounded" /\ queryKey = "a" /\ refStamp = 4 /\ lag = 1) =>
        pointRow = [present |-> TRUE, key |-> "a", value |-> "a3", stamp |-> 3]
    /\ (selector = "bounded" /\ refStamp = 4 /\ lag = 1) =>
        scanRows = {
            [key |-> "a", value |-> "a3", stamp |-> 3],
            [key |-> "b", value |-> "b4", stamp |-> 4]
        }
    /\ (selector = "bounded" /\ queryKey = "a" /\ refStamp = 1 /\ lag = 5) =>
        lowerBound = 0 /\ pointRow = [present |-> TRUE, key |-> "a", value |-> "a1", stamp |-> 1]
    /\ (selector = "exact_staleness" /\ queryKey = "a" /\ refStamp = 4 /\ lag = 1) =>
        pointRow = [present |-> TRUE, key |-> "a", value |-> "a3", stamp |-> 3]
    /\ (selector = "exact_staleness" /\ refStamp = 1 /\ lag = 2) =>
        pointRow = NoRow /\ scanRows = {}

Safety ==
    /\ PointReadMatchesWindow
    /\ AtSelectorUsesInclusiveUpperBound
    /\ WithinSelectorUsesInclusiveBounds
    /\ ExactSelectorUsesSingleStamp
    /\ BoundedSelectorUsesReferenceWindow
    /\ ExactStalenessSelectorUsesReferenceOffset
    /\ ScanRowsMatchWindow
    /\ ScanRowsCarryMetadata
    /\ ConcreteSelectorCases

Spec == Init /\ [][Next]_Vars

====
