# Pitch-parser impact by year

Analysis date: 2026-09-11. No production data was rebuilt or published.

## Verified pitch loss: original 2023–2025 regular-season archives

Each row compares the original suffix-only lexical behavior with the corrected appearance-aware export. Ordinary pitch entries include automatic balls/strikes but exclude pitcher-pickoff throws and `NoPitch` markers. The missing percentage uses the corrected pitch total as its denominator. Changed events compare the complete ordered sequence, including annotations.

| Year | Games affected / source games | Changed events | Old pitch entries | Corrected pitch entries | Recovered pitches | Missing % | Recovered pitcher pickoffs |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2023 | 2,377 / 2,430 | 23,436 | 646,012 | 720,683 | 74,671 | 10.36% | 497 |
| 2024 | 64 / 2,429 | 65 | 711,758 | 711,897 | 139 | 0.02% | 0 |
| 2025 | 2,430 / 2,430 | 36,407 | 605,722 | 712,523 | 106,801 | 14.99% | 899 |

All 683,446 events in these 7,289 games matched the original raw records under the new extraction rules. `NoPitch` counts remain unchanged. The validation report records four original count inconsistencies and one terminal-type mismatch separately from extraction correctness.

2024 supplies many more explicit `NP` records: 59,802, versus 27,446 in 2023 and 27,534 in 2025. This is consistent with the old truncation assumption working much more often in 2024. For example, ANA202404050 explicitly records the prefix before replaying it:

```text
play,1,0,dever001,32,CBBBS,NP
play,1,0,dever001,32,CBBBS.*B,W
```

The precise reason for different source-record conventions across years is not established by this analysis.

[Modern metrics CSV](pitch_impact_2023_2025.csv) · [Validation report](pitch_sequence_validation_2023_2025.json)

## Historical results after reconciliation

The complete audit now preserves all 205,886 parsed games. Of 18,141,020 event
rows, 333 belong to 144 unresolved appearances across 141 games. The remaining
issues are 144 pitch/pickoff-token disagreements and one catcher-pickoff-base
conflict. Every appearance is either resolved, unavailable, or unresolved;
all unresolved appearances have their normalized pitch rows quarantined.

1991 now has five unresolved appearances in five games; 1992 has four in four
games. The strict run stopped at each game's first conflict, so these final
counts include later disagreements it never reached. The 2023–2025 corpus
has no unresolved appearances, including postseason and All-Star files.

[Final per-year reconciliation CSV](pitch_history_reconciliation_by_year.csv)
contains separate appearance and event totals for all three statuses. A
resolved appearance can still have incomplete source coverage; unavailable
means it has no parsed sequence items. These totals include conventional and
deduced play-by-play; the strict comparison below uses conventional games with
nonempty pitch fields. Their denominators should not be compared directly.

## Historical conflicts under strict matching

This table measures a different issue: games rejected by strict cumulative-history matching. It is not a measurement of missing pitches in older seasons. The denominator is the number of distinct game IDs with at least one nonempty pitch field in the current local `retrosheet/**/*.EV*` inputs, including postseason and All-Star files. That scope is broader than the modern regular-season archives above.

Each rejected game is categorized by its first conflict; later conflicts in that game were not evaluated. An annotation-only first conflict does not establish that the rest of the game is free of token conflicts. Early-year pitch coverage is sparse, and a nonempty field can contain only a marker.

There are 3,124 rejected games out of 94,904 games with nonempty pitch fields. Of those, 2,908 first conflicts concern annotations and 216 concern pitch/pickoff token identities. 1991 and 1992 account for 2,839 rejections (90.88%); 2,835 of these are annotation omissions. Treating all metadata differences as fatal would therefore reject roughly two-thirds of those two seasons.

| Year | Games with pitch fields | Annotation-only first conflict | Pitch/pickoff first conflict | Total rejected | Rejected % |
|---|---:|---:|---:|---:|---:|
| 1908 | 5 | 0 | 0 | 0 | 0.00% |
| 1910 | 10 | 0 | 0 | 0 | 0.00% |
| 1911 | 26 | 0 | 0 | 0 | 0.00% |
| 1912 | 11 | 0 | 0 | 0 | 0.00% |
| 1913 | 32 | 0 | 0 | 0 | 0.00% |
| 1914 | 13 | 0 | 0 | 0 | 0.00% |
| 1915 | 22 | 0 | 0 | 0 | 0.00% |
| 1916 | 17 | 0 | 0 | 0 | 0.00% |
| 1917 | 40 | 0 | 0 | 0 | 0.00% |
| 1918 | 13 | 0 | 0 | 0 | 0.00% |
| 1919 | 53 | 0 | 0 | 0 | 0.00% |
| 1920 | 31 | 0 | 0 | 0 | 0.00% |
| 1921 | 166 | 1 | 1 | 2 | 1.20% |
| 1922 | 204 | 0 | 0 | 0 | 0.00% |
| 1923 | 258 | 0 | 0 | 0 | 0.00% |
| 1924 | 293 | 0 | 3 | 3 | 1.02% |
| 1925 | 265 | 0 | 1 | 1 | 0.38% |
| 1926 | 17 | 0 | 0 | 0 | 0.00% |
| 1927 | 105 | 0 | 0 | 0 | 0.00% |
| 1928 | 19 | 0 | 0 | 0 | 0.00% |
| 1929 | 156 | 0 | 0 | 0 | 0.00% |
| 1930 | 31 | 0 | 0 | 0 | 0.00% |
| 1931 | 10 | 0 | 0 | 0 | 0.00% |
| 1932 | 17 | 0 | 0 | 0 | 0.00% |
| 1933 | 25 | 0 | 0 | 0 | 0.00% |
| 1934 | 28 | 0 | 0 | 0 | 0.00% |
| 1935 | 14 | 0 | 0 | 0 | 0.00% |
| 1936 | 33 | 0 | 0 | 0 | 0.00% |
| 1937 | 12 | 0 | 0 | 0 | 0.00% |
| 1938 | 14 | 0 | 0 | 0 | 0.00% |
| 1939 | 15 | 0 | 0 | 0 | 0.00% |
| 1940 | 7 | 0 | 0 | 0 | 0.00% |
| 1941 | 22 | 0 | 1 | 1 | 4.55% |
| 1942 | 17 | 0 | 0 | 0 | 0.00% |
| 1943 | 9 | 0 | 0 | 0 | 0.00% |
| 1944 | 11 | 0 | 0 | 0 | 0.00% |
| 1945 | 15 | 1 | 0 | 1 | 6.67% |
| 1946 | 29 | 0 | 0 | 0 | 0.00% |
| 1947 | 172 | 0 | 1 | 1 | 0.58% |
| 1948 | 172 | 0 | 0 | 0 | 0.00% |
| 1949 | 177 | 0 | 0 | 0 | 0.00% |
| 1950 | 151 | 0 | 0 | 0 | 0.00% |
| 1951 | 153 | 0 | 2 | 2 | 1.31% |
| 1952 | 178 | 0 | 0 | 0 | 0.00% |
| 1953 | 169 | 0 | 1 | 1 | 0.59% |
| 1954 | 166 | 0 | 0 | 0 | 0.00% |
| 1955 | 165 | 0 | 0 | 0 | 0.00% |
| 1956 | 170 | 0 | 0 | 0 | 0.00% |
| 1957 | 170 | 0 | 0 | 0 | 0.00% |
| 1958 | 164 | 0 | 0 | 0 | 0.00% |
| 1959 | 173 | 0 | 0 | 0 | 0.00% |
| 1960 | 213 | 0 | 2 | 2 | 0.94% |
| 1961 | 187 | 0 | 1 | 1 | 0.53% |
| 1962 | 230 | 2 | 1 | 3 | 1.30% |
| 1963 | 293 | 0 | 0 | 0 | 0.00% |
| 1964 | 171 | 0 | 1 | 1 | 0.58% |
| 1965 | 57 | 0 | 0 | 0 | 0.00% |
| 1966 | 19 | 0 | 0 | 0 | 0.00% |
| 1967 | 24 | 0 | 0 | 0 | 0.00% |
| 1968 | 26 | 0 | 0 | 0 | 0.00% |
| 1969 | 109 | 4 | 1 | 5 | 4.59% |
| 1970 | 98 | 1 | 1 | 2 | 2.04% |
| 1971 | 128 | 0 | 1 | 1 | 0.78% |
| 1972 | 81 | 0 | 0 | 0 | 0.00% |
| 1973 | 32 | 0 | 0 | 0 | 0.00% |
| 1974 | 21 | 0 | 2 | 2 | 9.52% |
| 1975 | 99 | 0 | 1 | 1 | 1.01% |
| 1976 | 103 | 0 | 1 | 1 | 0.97% |
| 1977 | 176 | 0 | 1 | 1 | 0.57% |
| 1978 | 231 | 2 | 5 | 7 | 3.03% |
| 1979 | 190 | 0 | 2 | 2 | 1.05% |
| 1980 | 90 | 2 | 1 | 3 | 3.33% |
| 1981 | 76 | 0 | 4 | 4 | 5.26% |
| 1982 | 29 | 0 | 3 | 3 | 10.34% |
| 1983 | 18 | 1 | 1 | 2 | 11.11% |
| 1984 | 69 | 1 | 3 | 4 | 5.80% |
| 1985 | 85 | 0 | 2 | 2 | 2.35% |
| 1986 | 134 | 0 | 4 | 4 | 2.99% |
| 1987 | 72 | 1 | 2 | 3 | 4.17% |
| 1988 | 2,087 | 3 | 4 | 7 | 0.34% |
| 1989 | 2,063 | 5 | 32 | 37 | 1.79% |
| 1990 | 1,959 | 0 | 3 | 3 | 0.15% |
| 1991 | 2,123 | 1,405 | 1 | 1,406 | 66.23% |
| 1992 | 2,126 | 1,430 | 3 | 1,433 | 67.40% |
| 1993 | 2,222 | 0 | 3 | 3 | 0.14% |
| 1994 | 1,535 | 1 | 3 | 4 | 0.26% |
| 1995 | 1,916 | 1 | 0 | 1 | 0.05% |
| 1996 | 2,072 | 8 | 3 | 11 | 0.53% |
| 1997 | 2,142 | 4 | 2 | 6 | 0.28% |
| 1998 | 2,326 | 4 | 21 | 25 | 1.07% |
| 1999 | 2,429 | 0 | 35 | 35 | 1.44% |
| 2000 | 2,461 | 13 | 3 | 16 | 0.65% |
| 2001 | 2,465 | 3 | 13 | 16 | 0.65% |
| 2002 | 2,461 | 0 | 9 | 9 | 0.37% |
| 2003 | 2,469 | 3 | 2 | 5 | 0.20% |
| 2004 | 2,463 | 0 | 1 | 1 | 0.04% |
| 2005 | 2,462 | 0 | 0 | 0 | 0.00% |
| 2006 | 2,460 | 0 | 4 | 4 | 0.16% |
| 2007 | 2,460 | 0 | 0 | 0 | 0.00% |
| 2008 | 2,461 | 0 | 1 | 1 | 0.04% |
| 2009 | 2,461 | 0 | 5 | 5 | 0.20% |
| 2010 | 2,463 | 2 | 0 | 2 | 0.08% |
| 2011 | 2,468 | 0 | 3 | 3 | 0.12% |
| 2012 | 2,468 | 0 | 3 | 3 | 0.12% |
| 2013 | 2,470 | 2 | 1 | 3 | 0.12% |
| 2014 | 2,463 | 3 | 1 | 4 | 0.16% |
| 2015 | 2,466 | 2 | 4 | 6 | 0.24% |
| 2016 | 2,464 | 2 | 3 | 5 | 0.20% |
| 2017 | 2,469 | 1 | 2 | 3 | 0.12% |
| 2018 | 2,465 | 0 | 0 | 0 | 0.00% |
| 2019 | 2,467 | 0 | 0 | 0 | 0.00% |
| 2020 | 951 | 0 | 0 | 0 | 0.00% |
| 2021 | 2,467 | 0 | 1 | 1 | 0.04% |
| 2022 | 2,471 | 0 | 0 | 0 | 0.00% |
| 2023 | 2,472 | 0 | 0 | 0 | 0.00% |
| 2024 | 2,473 | 0 | 0 | 0 | 0.00% |
| 2025 | 2,478 | 0 | 0 | 0 | 0.00% |

[Complete historical CSV](pitch_history_conflicts_by_year.csv)

## Implications

The confirmed pitch-loss correction materially changes 2023 and 2025 pitch-derived data. 2024 needs a much smaller correction. Regeneration would include the pitch-sequence source, downstream pitch-statistics and pitch-observation tables, and the published database containing those tables. Preserve full-corpus event-key ordering; temporary season-only exports cannot be substituted directly.

Historical annotation reconciliation and true token conflicts are separate decisions. Exact pitch identity can potentially coexist with preservation of earlier annotations and updates to the original owning event when annotations arrive later. Conflicting actual pitch/pickoff sequences still require source evidence or an explicit unresolved-data policy. No such reconciliation or source-data repair has been performed.

See [pitch-history behavior and regeneration](pitch_sequence_pa_resume.md) for implementation and validation details.
