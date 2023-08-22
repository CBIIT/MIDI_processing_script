2023-04-04
baseline run of beta script 3-29-23 on 4-4-23 
ds 1.1 23
script is the same as previous alpha version 11-29-22 except changed to beta 
file number input: group 1 = 14662, group 2 = 9259
file number output: group 1 = 14662, group 2 = 9259


2023-04-19
baseline run of beta script 3-29-23 on 4-4-23 
ds 1.1 600
script is the same as previous alpha version 11-29-22 except changed to beta 
file number input: group 1 = 14662, group 2 = 9259
file number output: group 1 = 14662, group 2 = 9259


2023-05-15
used manual preprocessing script and beta script 5-5-23  on 5-15-23 (error in script name in gcp)
script is changed from previous run to automatically keep Media Storage SOP Instance UID,
and removed Device Serial Number from reset list
ds 1.1 23
file number input: 721
file number output: 721


2023-05-23
used automatic preprocessing script v1 and beta script 5-15-23 
ds 1.1 23
file number input:721
file number output:720


2023-06-05
used automatic pre-processing script v2 and beta script 5-15-23
ds 1.1 600
file number input: group 1 = 14662, group 2 = 9259
file number output: group 1 = 14662, group 2 = 9258


2023-06-27
used automatic pre-processing script v3 and beta script 6-26-23
ds 1.1 23
script is same as previous except station name and institution name are removed instead of reset
file input number: 721
file number output: 721
de-id time: 2 min 17 sec


2023-07-21
used automatic post-processing script v4 and script 7-20-23
ds 1.1 23
script is same as previous except patient id is untouched, dates aren't shifted and retrieve AE title is removed
file input number: 721
file output number: 721
de-id time: 3 min 25 sec



pre-processing scripts:

manual: flags all strings of integers lengths of 6 or longer.
the flagged strings are manually looked over, and patient ids and dates are removed

auto v1: removes all integer strings equal or longer than 6. removes dates of format yyyymmdd
auto v2: same as previous, but (incorrectly) attempted to remove code value
auto v3: same as previous, but code value is correctly removed, along with private creator.
dates in the format yyyymmdd, ddmmyyyy, and mmddyyyy are removed. also references keeplist from gcp script
auto v4: same as previous, but done in post. Patient ID is regenerated in the patient ID tag, but removed elsewhere.
dates are shifted (using random 1-100 number based on series ID) both in DA and DT tags, but also elsewehre using a 
new regex based date finding function that is more comprehensive than previous date finders, which only looked for 3 formats of dates. 
whitelist is used for location and initials regex expressions.
