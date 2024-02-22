SELECT name, project_count FROM (
    SELECT client.name, COUNT(client.id) as project_count FROM client
    JOIN project ON client.id=project.client_id
    GROUP BY client.id)
    WHERE project_count = (SELECT MAX(project_count) FROM (
        SELECT name, project_count FROM (
            SELECT client.name, COUNT(client.id) as project_count FROM client
            JOIN project ON client.id=project.client_id
            GROUP BY client.id)));