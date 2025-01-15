# Data sorting architectural decision record

## Problem

In our alpha prototype, we sorted all of the combined data by four fields:

- `data_collection_log_timestamp`
- `dno_alias`
- `secondary_substation_unique_id`
- `lv_feeder_unique_id`

This meant that the data from different DNOs was interleaved and the data was
fundamentally in time order. We did this after some not particularly extensive
conversations with users, which suggested time range querying/filtering of data was
probably the most common use case. We also thought that other querying (e.g. by
DNO, substation or other grid topology/hierarchy) would be better served by custom
partitions of the data, rather than sorting. Our hope though was that by explicitly
sorting the data, we would enable a variety of byte range queries for efficient
retrieval, even from a single file.

However, our prototype had the benefit of being able to load the entire data into memory
and sort it there. In our production system, we are processing data in batches, which
means we never have it all in memory. We did this because we know it is likely to grow
beyond the size of a single machine's memory, even for a single partition in our monthly
partitioning system.

This therefore means that we can't easily sort our data by the same criteria. It also
means that (at the time of writing) our combined output has a bug in the sense that it
_says_ the data is sorted this way in the parquet metadata, but in actual fact it's not,
because the input data is not sorted that way, and we're only applying any sorting
ourselves to the individual small batches as we process them.

## How is the raw data sorted?
Thinking about this made me realise that we have not necessarily been aware of how the
input is ordered. So I had a look:

- SSEN data is ordered by `dataset_id` (which is the same as sorting by substation and
  feeder ids) and then by `data_collection_log_timestamp`. Both ascending.
- NPg data is ordered by `secondary_substation_name`, (not id), then by
  `lv_feeder_name`/`id` (they appear to be the same values so it could be either), then
  by `data_collection_log_timestamp`. All ascending.
- NGED data appears to be grouped by `secondary_substation_id`, then ordered by
  `lv_feeder_ID` and `data_collection_log_timestamp`, but with no obvious ordering of
  the substations. Where it is ordered, it's ascending.
- UKPN data does not appear to be sorted at all.

## What are our limitations?
The runners Dagster provides us have 16GB of RAM, as does my laptop, so that seems like
a reasonable upper limit on what data needs to "fit into". Measuring a few of our
per-DNO monthly files in PyArrow, the largest take up approximately 9GB, so it feels
like that is a reasonable maximum. In other words, we can fit one DNO's monthly data in
memory, but not all of them. (Note also, only in PyArrow, a Pandas dataframe takes
up 27GB, so presumably a lot of swapping is going on there).

## Options

### Don't sort the data and just throw it all in together
This will make the data much more expensive to access, costing us and our users
bandwidth, and making the data much harder to use. It'll be much quicker for us though,
as we can happily stream/batch data in however we like, without any expensive sorting
step.

### Sort within DNO, then stick DNOs together
This is feasible in Dagster's runners - we can load the monthly data for each DNO in
memory, sort it, then write it. We could do that when we generate the monthly per-DNO
file, or when we load it in the combined geoparquet output.

What this means for our users is that our sorting essentially becomes `dno_alias`,
`data_collection_log_timestamp`, `secondary_substation_id`, `lv_feeder_id` instead. This
could have a knock on effect on the read performance of our data when filtering by time
because rows for the same timestamp are less likely to be in the same row groups.

### Sort witin DNO, then merge and re-sort daily batches when combining
Instead of reading an entire DNO's monthly file when combining data, we can selectively
pull the same day's worth of data from each. i.e. process 2024-01-01, 2024-01-02, etc in
order. This allows us to re-sort each daily combined batch in the way we want (timestamp
first), resulting in an output file that is completely ordered, without loading too much
data in memory at any one time.

## Conclusion
We will take the third approach - sorting each DNO's monthly file output, so that we can
then filter each one efficiently for 1 day's worth of data at a time, before combining
and sorting _that_.

