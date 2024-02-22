SELECT id, count_day FROM (
    SELECT project.id, (EXTRACT(DAY FROM finish_date - start_date)/(365/12)) as count_day FROM project) 
WHERE count_day = (SELECT MAX(count_day) FROM (
    SELECT project.id, (EXTRACT(DAY FROM finish_date - start_date)/(365/12)) as count_day FROM project));