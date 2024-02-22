SELECT type, name, birthday FROM (
    SELECT 'ELDEST' as type, name, birthday FROM worker 
    where birthday = (SELECT MIN(birthday) from worker)
    UNION 
    SELECT 'YOUNGEST' as type, name, birthday FROM worker 
    where birthday = (SELECT MAX(birthday) from worker));