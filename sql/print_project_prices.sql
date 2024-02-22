SELECT project_id, SUM(salary) as PRICE FROM project
INNER JOIN project_worker ON project.id = project_worker.project_id
INNER JOIN worker ON project_worker.worker_id = worker.id
GROUP BY project_id;