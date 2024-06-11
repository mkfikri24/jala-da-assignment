WITH
  ranked_ponds AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY ponds_id ORDER BY SR ASC, FCR ASC) AS row_num
  FROM (
    SELECT
      ponds.id AS ponds_id,
      cycles.id AS cycles_id,
      length,
      width,
      deep,
      total_seed,
      started_at,
      finished_at,
      area,
      padat_tebar,
      cycle_day_count,
      count_feed,
      sum_feed_weight,
      count_fasting,
      sum_size_per_kg,
      sum_weight_biomassa,
      sum_biomassa,
      avg_morning_temperature,
      avg_evening_temperature,
      avg_morning_do,
      avg_evening_do,
      avg_morning_salinity,
      avg_evening_salinity,
      avg_morning_pH,
      avg_evening_pH,
      avg_transparency,
      avg_ammonia,
      avg_nitrate,
      avg_nitrite,
      avg_alkalinity,
      avg_hardness,
      avg_calcium,
      avg_magnesium,
      avg_carbonate,
      avg_bicarbonate,
      avg_tom,
      avg_total_plankton,
      ADG,
      ((sum_biomassa / total_seed) * 100) AS SR,
      (sum_feed_weight / sum_weight_biomassa) AS FCR
    FROM
      jala-da-test.jala.ponds AS ponds
    INNER JOIN (
      SELECT
        *,
        (total_seed / area) AS padat_tebar,
        DATETIME_DIFF(finished_at, started_at, DAY) AS cycle_day_count
      FROM
        jala-da-test.jala.cycles) AS cycles
    ON
      ponds.id = cycles.pond_id
    INNER JOIN (
      SELECT
        cycle_id AS cycle_id_feed,
        COUNT(logged_at) AS count_feed,
        SUM(quantity) AS sum_feed_weight
      FROM
        jala-da-test.jala.feeds
      GROUP BY
        cycle_id) AS total_feed
    ON
      cycles.id = total_feed.cycle_id_feed
    LEFT JOIN (
      SELECT
        cycle_id AS cycle_id_fasting,
        SUM(fasting) AS count_fasting
      FROM
        `jala-da-test.jala.fasting`
      GROUP BY
        1 ) AS fasting
    ON
      cycles.id = fasting.cycle_id_fasting
    INNER JOIN (
      SELECT
        cycle_id AS cycle_id_harvest,
        SUM(size) AS sum_size_per_kg,
        SUM(weight) AS sum_weight_biomassa,
        SUM(size * weight) AS sum_biomassa
      FROM
        jala-da-test.jala.harvests AS A
      INNER JOIN
        jala-da-test.jala.cycles AS B
      ON
        A.cycle_id = B.id
      WHERE
        DATE(B.finished_at) = DATE(A.harvested_at)
        AND A.status != 'Failed'
      GROUP BY
        cycle_id_harvest) AS total_harvest
    ON
      cycles.id = total_harvest.cycle_id_harvest
    INNER JOIN (
      SELECT
        cycle_id AS cycle_id_mea,
        AVG(morning_temperature) avg_morning_temperature,
        AVG(evening_temperature) avg_evening_temperature,
        AVG(morning_do) avg_morning_do,
        AVG(evening_do) avg_evening_do,
        AVG(morning_salinity) avg_morning_salinity,
        AVG(evening_salinity) avg_evening_salinity,
        AVG(morning_pH) avg_morning_pH,
        AVG(evening_pH) avg_evening_pH,
        AVG(transparency) avg_transparency,
        AVG(ammonia) avg_ammonia,
        AVG(nitrate) avg_nitrate,
        AVG(nitrite) avg_nitrite,
        AVG(alkalinity) avg_alkalinity,
        AVG(hardness) avg_hardness,
        AVG(calcium) avg_calcium,
        AVG(magnesium) avg_magnesium,
        AVG(carbonate) avg_carbonate,
        AVG(bicarbonate) avg_bicarbonate,
        AVG(tom) avg_tom,
        AVG(total_plankton_) avg_total_plankton
      FROM
        `jala-da-test.jala.measurements`
      GROUP BY
        1 ) AS mea
    ON
      cycles.id = mea.cycle_id_mea
    INNER JOIN (
      WITH
        diff_table AS (
        SELECT
          cycle_id AS cycle_id_sampling,
          TIMESTAMP(sampled_at) AS sampled_at,
          average_weight,
          LAG(TIMESTAMP(sampled_at)) OVER (PARTITION BY cycle_id ORDER BY sampled_at) AS prev_sampled_at,
          LAG(average_weight) OVER (PARTITION BY cycle_id ORDER BY sampled_at) AS prev_average_weight
        FROM
          jala-da-test.jala.samplings
        ORDER BY
          1,
          2 ASC)
      SELECT
        cycle_id_sampling,
        AVG((average_weight - prev_average_weight) / DATE_DIFF(sampled_at, prev_sampled_at, DAY)) AS ADG
      FROM
        diff_table
      WHERE
        prev_sampled_at IS NOT NULL
        AND prev_average_weight IS NOT NULL
        AND DATE_DIFF(sampled_at, prev_sampled_at, SECOND) > 0
      GROUP BY
        cycle_id_sampling ) AS avg_adg
    ON
      cycles.id = avg_adg.cycle_id_sampling
    WHERE
      sum_weight_biomassa != 0 ) AS subquery
  WHERE
    SR < 100
    OR FCR < 20 )

SELECT
  * EXCEPT (row_num)
FROM
  ranked_ponds
WHERE
  row_num = 1;
