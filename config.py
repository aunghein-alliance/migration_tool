# User configuration

SERVER = r"DESKTOP-PGA3RL7\SQLEXPRESS"
DATABASE = "analytical_alliance"
SCHEMA = "dbo"

ENCRYPT = "yes"
TRUST_CERT = "yes"

TRUNCATE_BEFORE_INSERT = False
SET_DATEFORMAT_DMY = False
TRUNCATE_OVERLONG_STRINGS = True
REPORT_TOP_N_OFFENDERS = 5
FILL_BLANK_NUMERIC_WITH_ZERO = True
COMMON_DELIMITERS = [",", "\t", ";", "|"]

# Table / CSV configurations
TABLE_CONFIG = [
    {
        "csv_path": r"C:\Users\IT\Downloads\MIS Savings June 2025.csv",
        "table_name": "MIS_SVACC",
        "columns": [
            "MIS_DAT", "GRP_NM", "ACC", "CO_ID", "CO_NM",
            "PRD_COD", "PRD_NM", "PRD_CAT",
            "CID", "ACC_NO",
            "BR_COD", "BR_NM", "PR_BR", "RG_NM",
            "OPN_DAT", "SAL_NO",
            "CUS_NM", "GENDER", "FATH_NM",
            "AR_TY", "AR",
            "VILL_WAD", "VILL_TRT_TW", "CIT_TWN", "DIST", "RG_STT",
            "NRC",
            "MBNO_1", "MBNO_2",
            "CUS_STAT", "FREZ_STAT",
            "DISB_AMT", "LPF_AMT",
            "INST_NO", "INST_AMT",
            "PAY_FREQ",
            "PRINCIPAL", "IR",
            "NON_CR_CUS",
            "VOL_DEPO", "POVT_SCORE", "HH_SURPLUS_INC",
            "PURPOSE", "BIZ_CAT", "BIZ_ACTIVITY",
            "ACC_STAT",
            "MAT_DAT",
            "PAR_CLIENT",
            "OVD_DAY",
            "AREA_STATUS"
        ],
        "date_cols": ["MIS_DAT", "OPN_DAT", "MAT_DAT"],
        "int_cols": ["SAL_NO", "INST_NO", "OVD_DAY"],
        "dec_18_2": ["DISB_AMT", "LPF_AMT", "INST_AMT", "PRINCIPAL", "VOL_DEPO", "HH_SURPLUS_INC"],
        "dec_10_2": ["IR", "POVT_SCORE"],
        "text_cols": ["ACC"]
    }

    # Add more table/csv configs here
]
