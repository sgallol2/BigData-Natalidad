-- ============================================================
-- Pregunta 5: Comparativa de natalidad internacional (Banco Mundial Data360)
-- Objetivo: Comparar la tasa bruta de natalidad (nacimientos por cada 1.000 habitantes)
--           de Colombia frente a otros países de la región.
-- ============================================================

SELECT 
    wb.pais,
    wb.anio,
    ROUND(wb.valor, 2) AS tasa_bruta_natalidad
FROM natalidad_db.trusted_world_bank wb
WHERE wb.indicador = 'WB_WDI_SP_DYN_CBRT_IN'
ORDER BY wb.anio DESC, tasa_bruta_natalidad DESC;
