-- ============================================================
-- Pregunta 4: Relación entre nivel educativo de la madre, controles prenatales y tipo de parto
-- Objetivo: Evaluar si mayor nivel educativo se asocia a más controles prenatales
--           y analizar la proporción de partos por cesárea vs espontáneo.
-- ============================================================

SELECT 
    COALESCE(cat_edu.descripcion, 'Otro / No especificado') AS nivel_educativo,
    COUNT(*) AS total_nacimientos,
    ROUND(AVG(n.numconsul), 2) AS promedio_controles_prenatales,
    ROUND(100.0 * COUNT(CASE WHEN n.tipo_parto = '2' THEN 1 END) / COUNT(*), 2) AS pct_cesareas,
    ROUND(100.0 * COUNT(CASE WHEN n.tipo_parto = '1' THEN 1 END) / COUNT(*), 2) AS pct_espontaneos
FROM natalidad_db.trusted_nacimientos n
LEFT JOIN natalidad_db.dim_catalogo cat_edu 
    ON cat_edu.variable = 'NIV_EDUM' 
    AND (n.niv_edum = cat_edu.codigo OR LPAD(n.niv_edum, 2, '0') = cat_edu.codigo)
WHERE n.niv_edum IS NOT NULL
GROUP BY cat_edu.descripcion
ORDER BY promedio_controles_prenatales DESC;
