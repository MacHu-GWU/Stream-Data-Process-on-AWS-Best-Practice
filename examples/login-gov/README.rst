login.gov Example
==============================================================================

.. contents::
    :depth: 1
    :local:

login.gov wants to build a real time dashboard to answer:

1. How many new sign up we have for this month?
2. How many users we have so far?
3. Show me the hourly trends of new sign up counts.


First, let's analysis this problems
------------------------------------------------------------------------------

login.gov events data, 1 M events per day:

::

    +--------------------------------------+----------------------------+------------+
    | event_id                             | event_time                 | event_name |
    +--------------------------------------+----------------------------+------------+
    | 8a203447-55b3-47d6-b016-344235e40d01 | 2020-02-12 18:06:01.021803 | sign_in    |
    +--------------------------------------+----------------------------+------------+
    | 4f629b38-ea66-40e5-8eb0-659b817b0dec | 2020-02-12 18:06:03.494509 | sign_in    |
    +--------------------------------------+----------------------------+------------+
    | bcb51c27-b981-463e-9533-e44f5b95fe55 | 2020-02-12 18:06:06.620111 | sign_up    |
    +--------------------------------------+----------------------------+------------+
    | ...                                  |                            |            |
    +--------------------------------------+----------------------------+------------+

How many user signed up so far?

::

    SELECT COUNT(*)
    FROM events
    WHERE
        events.event_name = "sign_up"

How many new user signed up this month so far?

::

    SELECT COUNT(*)
    FROM events
    WHERE
        events.event_time >= DATETIME(TODAY().month, 1, 1)
        AND events.event_name = "sign_up"

Show me the daily trends of new sign up counts.

::

    SELECT
        event_time - TIMESTAMP '1970-01-01 00:00:00') SECOND / 3600 TO SECOND) * 3600 + TIMESTAMP '1970-01-01 00:00:00' as the_hour,
        COUNT(*) as sign_up_event_counts
    FROM events
    WHERE event_name = 'sign_up'
    GROUP BY
        event_time - TIMESTAMP '1970-01-01 00:00:00') SECOND / 3600 TO SECOND) * 3600 + TIMESTAMP '1970-01-01 00:00:00';

All of them are a full table scan. To get the live data, you can not run this every seconds.


Seconds, the Kinesis Analytics Solution
------------------------------------------------------------------------------

1. Stream Data Input: kinesis stream
2. Stream Data Analytics / Processing: Kinesis Analytics (and AWS Lambda, not used in this case)
3. Stream Result Output: firehose delivery stream
4. Data Storage: S3 Bucket
5. Query Engine: Athena
6. Dashboard: QuickSight
7. Further custom business logic: AWS Lamnbda


Third, highlight of Kinesis Analytics
------------------------------------------------------------------------------

1. all serverless, no need to maintain VM, pay as you go.
2. can be up in minutes. kafka / spark solution takes weeks to setup the infrastructure.
3. data persist on S3 forever.


Final, what else we can do with Real-Time analysis
------------------------------------------------------------------------------

1. business metrics real time report. (Athena + QuickSight)
2. social media tweeter hot topic monitor. or celebrity could use it to monitor negative news crisis. (Simple Notification Service + Nature Language Process)
3. fraud detection for banking and credit cards. (Kinesis Analytics ML)
4. system monitoring. (CloudWatch + AWS Lambda + SNS)
5. stock trading system automation. (Redshift + AWS Lambda)
6. etc ...
