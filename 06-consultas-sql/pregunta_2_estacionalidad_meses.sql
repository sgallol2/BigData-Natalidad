-- ============================================================
-- Pregunta 2: Estacionalidad de nacimientos por mes
-- Objetivo: Determinar qué meses concentran más nacimientos en el país.
-- ============================================================

SELECT 
    n.mes,
    CASE n.mes
        WHEN 1 THEN 'Enero'
        WHEN 2 THEN 'Febrero'
        WHEN 3 THEN 'Marzo'
        WHEN 4 THEN 'Abril'
        WHEN 5 THEN 'Mayo'
        WHEN 6 THEN 'Junio'
        WHEN 7 THEN 'Julio'
        WHEN 8 THEN 'Agosto'
        WHEN 9 THEN 'Septiembre'
        WHEN 10 THEN 'Octubre'
        WHEN 11 THEN 'Noviembre'
        WHEN 12 THEN 'Diciembre'
        ELSE 'Desconocido'
    END AS nombre_mes,
    COUNT(*) AS total_nacimientos,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT n.anio), 0) AS promedio_anual
FROM natalidad_db.trusted_nacimientos n
WHERE n.mes IS NOT NULL
GROUP BY n.mes
ORDER BY total_nacimientos DESC;
