import pandas as pd
from datetime import datetime

date_time = datetime.now()

document_type = pd.read_csv("files/relations/open_ar/document_type.csv")
customer_ids = pd.read_csv("files/relations/customers/ids.csv")


def getMapping(old_value, relation_csv, default_value):
    new_value = old_value
    # print("starting_value", new_value)
    if (new_value != ""):
        find_new = relation_csv.loc[relation_csv["old_value"]
                                    == new_value]['new_value']
        # print("empty: ", find_new.empty)
        if (not find_new.empty):
            new_value = relation_csv.loc[relation_csv["old_value"]
                                         == new_value]['new_value'].values[0]
        else:
            new_value = default_value
    else:
        new_value = default_value

    # print("returning", new_value)
    return (new_value)


def read_transform():
    df = pd.read_excel(
        "files/from_southware/OPENAR01-Southware-11-30-2022-339pm.xlsx")
    df = df[df["CURRENT"] != 0]
    print(df)
    # df = df.sample(n=1000, random_state=1)
    # Transform DATA

    # df["ZIP Code"] = df["Zip Code"].apply(cleanPhoneNo)
    # df["Phone No."] = df["Phone Number"].apply(cleanPhoneNo)
    # df["Fax No."] = df["Fax No"].apply(cleanPhoneNo)
    # df["Email"] = df["EMail Address"].apply(cleanEmail)
    # # df["phone_sanitized"] = df["phone_sanitized"].apply(cleanPhoneNo)

    get_mapping_columns = [
        {"bc_column": "Document Type", "southware_column": "Document Type Lit",
            "mapping_table": document_type, "default_value": ""},
        {"bc_column": "Account No.", "southware_column": "Customer Number",
         "mapping_table": customer_ids, "default_value": "not_found"},
    ]

    for mapping_column in get_mapping_columns:
        print("Setting mapping for column: ", mapping_column["bc_column"])
        df[mapping_column["bc_column"]] = df[mapping_column["southware_column"]].apply(getMapping,
                                                                                       args=(mapping_column["mapping_table"], mapping_column["default_value"],))

    df["Line No."] = range(10000, (10000*len(df))+1, 10000)
    df["Document No."] = "SAC" + df["Document Number"]
    df["External Document No."] = "SAC" + df["Document Number"]

    df.rename(columns={"Document Reference": "Description", "Total Amt Due": "Amount",
              "Doc Due Date": "Due Date", "Document Date": "Document Date"}, inplace=True)

    df["Journal Template Name"] = "CASHRCPT"
    df["Journal Batch Name"] = "DEFAULT"
    df["Account Type"] = "Customer"
    df["Bal. Account No."] = "11000"
    df["Posting Date"] = ""
    df["Shortcut Dimension 1 Code"] = "SAC"
    df["Gen. Bus. Posting Group"] = ""
    df["Gen. Prod. Posting Group"] = ""
    df["Bal. Account Type"] = "G/L Account"
    df["Dimension Set ID"] = ""
    df["Comment"] = ""
    df["Invoice No."] = ""

    df_columns = ["Journal Template Name", "Journal Batch Name", "Line No.", "Account Type", "Account No.", "Posting Date", "Document Type", "Document No.", "Description", "Bal. Account No.", "Amount",
                  "Shortcut Dimension 1 Code", "Due Date", "Gen. Bus. Posting Group", "Gen. Prod. Posting Group", "Bal. Account Type", "Document Date", "External Document No.", "Dimension Set ID", "Comment", "Invoice No."]

    df = df.reindex(columns=df_columns)

    print(df)

    df.to_excel("files/to_bc_outputs/openar_output.xlsx", index=False)
    df.to_csv("files/to_bc_outputs/openar_output.csv", index=False)
    df.to_csv("files/to_bc_outputs/history/openar_output_" +
              date_time.strftime("%m%d%y_%H%M%S") + ".csv.gz", index=False, compression='gzip')


read_transform()
