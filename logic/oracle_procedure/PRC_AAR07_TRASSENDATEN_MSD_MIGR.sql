CREATE OR REPLACE PROCEDURE CDIP_IN.PRC_AAR07_TRASSEN_MSD_MIGR
IS
l_count number;
l_procedure_name VARCHAR2(100);
BEGIN

l_procedure_name := $$PLSQL_UNIT;

-- Reset previous runs
UPDATE
	cdip_out.T_GP_FINAL_AZ_2310_V103
SET
	AB_CLUSTER = NULL,
	AB_MUFFEN = NULL,
	AB_TRASSEN_VERTRIEB = NULL,
	AB_PLANTRASSEN_NICHT_WEBOM = NULL,
	AB_PLANTRASSEN_NORMAL = NULL,
	AB_PLANTRASSEN_5G = NULL,
	AB_TRASSEN_ALL = NULL,
	AB_TRASSEN_FREMD = NULL,
	AB_TRASSEN_FREMD_BT = NULL,
	AB_TRASSEN_FREMD_OHNE_BT = NULL,
	AB_TRASSEN_OWN = NULL,
	AB_TRASSEN_OWN_BT = NULL,
	AB_TRASSEN_OWN_OHNE_BT = NULL,
	X = NULL,
	Y = NULL,
	PLZ_GEO = NULL,
	ORT_GEO = NULL,
	STRASSE_HNR_GEO = NULL,
	PLZ_ORIG = NULL,
	ORT_ORIG = NULL,
	STRASSE_HNR_ORIG = NULL,
	OWN_LR_PEC = NULL,
	LR_TYP = NULL,
	LR_DAT = NULL,
	FREMD_LR_PEC = NULL,
	AGS = NULL,
	GEN = NULL,
	BEZ = NULL,
	CLUSTER_ID = NULL,
	FLAG_STRASSENSEITE = NULL,
	FLAG_GUELTIG = NULL,
	ETL_TIMESTAMP = NULL
;
commit;



    MERGE INTO CDIP_OUT.T_GP_FINAL_AZ_2310_V103 final
USING (
SELECT  DISTINCT
            "AB_CLUSTER",
            "AB_MUFFEN",
            "AB_TRASSEN_VERTRIEB",
            "AB_PLANTRASSEN_NICHT_WEBOM",
            "AB_PLANTRASSEN_NORMAL",
            "AB_PLANTRASSEN_5G",
            "AB_TRASSEN_ALL",
            "AB_TRASSEN_FREMD",
            "AB_TRASSEN_FREMD_BT",
            "AB_TRASSEN_FREMD_OHNE_BT",
            "AB_TRASSEN_OWN",
            "AB_TRASSEN_OWN_BT",
            "AB_TRASSEN_OWN_OHNE_BT",
            "X",
            "Y",
            "PLZ_GEO",
            "ORT_GEO",
            "STRASSE_HNR_GEO",
            "PLZ_INPUT" PLZ_ORIG,
            "ORT_INPUT" ORT_ORIG,
            "STRASSE_HNR_INPUT" STRASSE_HNR_ORIG,
            "OWN_LR_PEC",
            "LR_TYP",
            "LR_DAT",
            "FREMD_LR_PEC",
            "AGS",
            "GEN",
            "BEZ",
            "CLUSTER_ID",
            "FLAG_STRASSENSEITE",
            "FLAG_GUELTIG",
            "ETL_TIMESTAMP"
    FROM ARCGIS.TRASSEN_DATEN_IN
    where PVID IN 
    (
     select distinct PVID 
        from (
        SELECT distinct  
        max(pvid) pvid, UPPER(plz_input || ' ' ||   ort_input || ' ' ||  strasse_hnr_input)
        FROM ARCGIS.TRASSEN_DATEN_IN 
        group by UPPER(plz_input || ' ' ||   ort_input || ' ' ||  strasse_hnr_input)
        )
        )
        ) ivtr 
ON (UPPER(final."PLZ")     =   UPPER(to_char(ivtr."PLZ_ORIG"))
    AND UPPER(final."ORT")     =   UPPER(ivtr."ORT_ORIG")
    AND UPPER(final."STRASSE") || ' ' || UPPER(final."HAUSNUMMER") = UPPER(ivtr."STRASSE_HNR_ORIG")
    )
WHEN MATCHED THEN
    UPDATE SET 
        final."AB_CLUSTER"=ivtr."AB_CLUSTER",
        final."AB_MUFFEN"=ivtr."AB_MUFFEN",
        final."AB_TRASSEN_VERTRIEB"=ivtr."AB_TRASSEN_VERTRIEB",
        final."AB_PLANTRASSEN_NICHT_WEBOM"=ivtr."AB_PLANTRASSEN_NICHT_WEBOM",
        final."AB_PLANTRASSEN_NORMAL"=ivtr."AB_PLANTRASSEN_NORMAL",
        final."AB_PLANTRASSEN_5G"=ivtr."AB_PLANTRASSEN_5G",
        final."AB_TRASSEN_ALL"=ivtr."AB_TRASSEN_ALL",
        final."AB_TRASSEN_FREMD"=ivtr."AB_TRASSEN_FREMD",
        final."AB_TRASSEN_FREMD_BT"=ivtr."AB_TRASSEN_FREMD_BT",
        final."AB_TRASSEN_FREMD_OHNE_BT"=ivtr."AB_TRASSEN_FREMD_OHNE_BT",
        final."AB_TRASSEN_OWN"=ivtr."AB_TRASSEN_OWN",
        final."AB_TRASSEN_OWN_BT"=ivtr."AB_TRASSEN_OWN_BT",
        final."AB_TRASSEN_OWN_OHNE_BT"=ivtr."AB_TRASSEN_OWN_OHNE_BT",
        final."X"=ivtr."X",
        final."Y"=ivtr."Y",
        final."PLZ_GEO"=ivtr."PLZ_GEO",
        final."ORT_GEO"=ivtr."ORT_GEO",
        final."STRASSE_HNR_GEO"=ivtr."STRASSE_HNR_GEO",

        final."PLZ_ORIG"=ivtr."PLZ_ORIG",
        final."ORT_ORIG"=ivtr."ORT_ORIG",
        final."STRASSE_HNR_ORIG"=ivtr."STRASSE_HNR_ORIG",

        final."OWN_LR_PEC"=ivtr."OWN_LR_PEC",
        final."LR_TYP"=ivtr."LR_TYP",
        final."LR_DAT"=ivtr."LR_DAT",
        final."FREMD_LR_PEC"=ivtr."FREMD_LR_PEC",
        final."AGS"=ivtr."AGS",
        final."GEN"=ivtr."GEN",
        final."BEZ"=ivtr."BEZ",
        final."CLUSTER_ID"=ivtr."CLUSTER_ID",
        final."FLAG_STRASSENSEITE"=ivtr."FLAG_STRASSENSEITE",
        final."FLAG_GUELTIG"=ivtr."FLAG_GUELTIG", 
        final."ETL_TIMESTAMP"=to_char(ivtr."ETL_TIMESTAMP")
        ;

l_count := SQL%ROWCOUNT;
COMMIT;

DBMS_OUTPUT.PUT_LINE('Affected Records Count: ' || l_count);

INSERT INTO CDIP_OUT.t_cdip_log (PROCEDURE_NAME, EXECUTION_DATE, LOG_DESCRIPTION)
VALUES (l_procedure_name, SYSDATE, 'Affected Records Count: ' || l_count);
COMMIT;

END;