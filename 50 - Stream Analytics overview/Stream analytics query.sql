SELECT
    *
INTO
    [PassthroughOutput]
FROM
    [myfirsteventhub]

SELECT
    EpisodeId,
    System.Timestamp() AS WindowEndTime,
    COUNT(*) AS ScoreCount,
    AVG(Score) AS AverageScore
INTO
    [TumblingOutput] 
FROM
    [myfirsteventhub]
GROUP BY
    EpisodeId, TumblingWindow(second, 5)

SELECT
    EpisodeId,
    System.Timestamp() AS WindowEndTime,
    COUNT(*) AS ScoreCount,
    AVG(Score) AS AverageScore
INTO
    [HoppingOutput] 
FROM
    [myfirsteventhub]
GROUP BY
    EpisodeId, HoppingWindow(second, 10, 5)


SELECT
    EpisodeId,
    System.Timestamp() AS WindowEndTime,
    COUNT(*) AS ScoreCount,
    AVG(Score) AS AverageScore
INTO
    [SlidingOutput] 
FROM
    [myfirsteventhub]
GROUP BY
    EpisodeId, SlidingWindow(second, 20)
HAVING
    AVG(Score) > 3 AND COUNT(*) > 3