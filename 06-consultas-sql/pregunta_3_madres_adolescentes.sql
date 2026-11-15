-- ============================================================
-- Pregunta 3: Proporción de nacimientos en madres adolescentes por departamento
-- Objetivo: Medir qué proporción de partos corresponde a menores de 19 años
--           (EDAD_MADRE grupos 1: 10-14 años y 2: 15-19 años).
-- ============================================================

SELECT 
    d.nombre AS departamento,
    COUNT(*) AS total_nacimientos,
    COUNT(CASE WHEN n.edad_madre IN ('1', '2') THEN 1 END) AS nacimientos_adolescentes,
    ROUND(100.0 * COUNT(CASE WHEN n.edad_madre IN ('1', '2') THEN 1 END) / COUNT(*), 2) AS pct_madres_adolescentes
FROM natalidad_db.trusted_nacimientos n
LEFT JOIN natalidad_db.dim_departamento d 
    ON n.cod_dpto = d.cod_dpto
WHERE n.edad_madre IS NOT NULL AND d.nombre IS NOT NULL
GROUP BY d.nombre
ORDER BY pct_madres_adolescentes DESC;
