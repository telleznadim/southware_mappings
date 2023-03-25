import pandas as pd
from datetime import datetime


def transformAp(df):
    df["Document Type"] = "Invoice" if (
        df["Amount"] >= 0) else "Credit Memo"
    return (df)


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


def read_transform(date_time, relations_dict, onedrive_path):
    df = pd.read_excel(
        onedrive_path + "Complete Master/OPENAR01-SAC-01-27-2023-458PM.xlsx")
    # df = df[df["CURRENT"] != 0]
    print(df)
    # df = df.sample(n=1000, random_state=1)
    # Transform DATA

    # df["ZIP Code"] = df["Zip Code"].apply(cleanPhoneNo)
    # df["Phone No."] = df["Phone Number"].apply(cleanPhoneNo)
    # df["Fax No."] = df["Fax No"].apply(cleanPhoneNo)
    # df["Email"] = df["EMail Address"].apply(cleanEmail)
    # # df["phone_sanitized"] = df["phone_sanitized"].apply(cleanPhoneNo)

    get_mapping_columns = [
        # {"bc_column": "Document Type", "southware_column": "Document Type Lit",
        #     "mapping_table": document_type, "default_value": ""},
        {"bc_column": "Account No.", "source_column": "Customer Number",
         "mapping_table": relations_dict["customer_ids"], "default_value": "not_found"},
        {"bc_column": "Customer Type InventedColumn", "source_column": "Customer Number",
         "mapping_table": relations_dict["intercompany_customers"], "default_value": "STD"},
        {"bc_column": "Bal. Account No.", "source_column": "Customer Type InventedColumn",
         "mapping_table": relations_dict["bal_account"], "default_value": "11000"},
    ]

    for mapping_column in get_mapping_columns:
        print("Setting mapping for column: ", mapping_column["bc_column"])
        df[mapping_column["bc_column"]] = df[mapping_column["source_column"]].apply(getMapping,
                                                                                    args=(mapping_column["mapping_table"], mapping_column["default_value"],))

    df["Line No."] = range(10000, (10000*len(df))+1, 10000)
    df["Document No."] = "SAC" + df["Document Number"]
    df["External Document No."] = "SAC" + df["Document Number"]
    df["Amount"] = (df["CURRENT"] + df["1-30 DAYS"] +
                    df["31-60 DAYS"] + df["OVER 60 DAYS"])
    df = df.apply(transformAp, axis=1)

    df.rename(columns={"Document Reference": "Description",
              "Doc Due Date": "Due Date", "Document Date": "Document Date"}, inplace=True)

    df["Journal Template Name"] = "CASHRCPT"
    df["Journal Batch Name"] = "SACIMPORT"
    df["Account Type"] = "Customer"
    # df["Bal. Account No."] = "11000"
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

    print(df[df.duplicated(subset=['Document No.'], keep=False)]["Document No."])
    df.to_excel(onedrive_path + "Download Southware/openar_output_" +
                date_time.strftime("%m%d%y") + ".xlsx", index=False)
    df.to_csv(onedrive_path + "Download Southware/openar_output_" +
              date_time.strftime("%m%d%y") + ".csv", index=False)
    df.to_excel("files/to_bc_outputs/openar_output.xlsx", index=False)
    df.to_csv("files/to_bc_outputs/openar_output.csv", index=False)
    df.to_csv("files/to_bc_outputs/history/openar_output_" +
              date_time.strftime("%m%d%y_%H%M%S") + ".csv.gz", index=False, compression='gzip')


def main():
    date_time = datetime.now()
    bal_account = pd.read_csv("files/relations/open_ar/bal_account.csv")
    customer_ids = pd.read_csv("files/relations/customers/ids.csv")
    intercompany_customers = pd.read_excel(
        "files/relations/customers/intercompany_customers.xlsx")
    relations_dict = {
        "customer_ids": customer_ids, "intercompany_customers": intercompany_customers, "bal_account": bal_account}

    onedrive_path = "/Users/nadimtellezbarrera/Library/CloudStorage/OneDrive-EVI/07-SAC/Go Live/5. Open AR/"
    read_transform(date_time, relations_dict, onedrive_path)


if __name__ == "__main__":
    main()
