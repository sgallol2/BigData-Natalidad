-- ============================================================
-- Pregunta 1: Evolución de nacimientos por año y departamento (2021 - 2025)
-- Objetivo: Identificar la tendencia temporal de nacimientos en Colombia
--           y qué departamentos concentran el mayor volumen.
-- ============================================================

SELECT 
    n.anio,
    d.nombre AS departamento,
    COUNT(*) AS total_nacimientos
FROM natalidad_db.trusted_nacimientos n
LEFT JOIN natalidad_db.dim_departamento d 
    ON n.cod_dpto = d.cod_dpto
WHERE n.cod_dpto IS NOT NULL
GROUP BY n.anio, d.nombre
ORDER BY n.anio ASC, total_nacimientos DESC;
