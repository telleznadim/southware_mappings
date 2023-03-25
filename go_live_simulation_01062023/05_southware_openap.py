import pandas as pd
from datetime import datetime

date_time = datetime.now()

document_type = pd.read_csv("files/relations/open_ar/document_type.csv")
vendor_ids = pd.read_csv("files/relations/vendors/ids.csv")


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


def transformAp(df):

    df["Document Type"] = "Credit Memo" if (
        df["Amount"] >= 0) else "Invoice"

    return (df)


def read_transform(onedrive_path):
    df = pd.read_excel(
        onedrive_path + "Complete Master/OPENAP01-SAC-01-09-2023.xlsx")
    # df = df[df["Current"] != 0]
    print(df)

    # df = df.sample(n=1000, random_state=1)
    # Transform DATA

    # df["ZIP Code"] = df["Zip Code"].apply(cleanPhoneNo)
    # df["Phone No."] = df["Phone Number"].apply(cleanPhoneNo)
    # df["Fax No."] = df["Fax No"].apply(cleanPhoneNo)
    # df["Email"] = df["EMail Address"].apply(cleanEmail)
    # # df["phone_sanitized"] = df["phone_sanitized"].apply(cleanPhoneNo)

    get_mapping_columns = [
        {"bc_column": "Account No.", "southware_column": "Vendor Number",
         "mapping_table": vendor_ids, "default_value": "not_found"},
    ]

    for mapping_column in get_mapping_columns:
        print("Setting mapping for column: ", mapping_column["bc_column"])
        df[mapping_column["bc_column"]] = df[mapping_column["southware_column"]].apply(getMapping,
                                                                                       args=(mapping_column["mapping_table"], mapping_column["default_value"],))


# Amount
    df["Amount"] = (df["Current"] + df["1 - 30 Days"] +
                    df["31 - 60 Days"] + df["Over 60 Days"]) * (-1)
    df = df.apply(transformAp, axis=1)
    df["Line No."] = range(10000, (10000*len(df))+1, 10000)
    df["Document No."] = "SAC" + df["Invoice Number"]
    df["External Document No."] = "SAC" + df["Invoice Number"]

    df.rename(columns={"Trans#": "Description",
              "G/L Account": "Bal. Account Type"}, inplace=True)

    df["Journal Template Name"] = "PAYMENT"
    df["Journal Batch Name"] = "SACIMPORT"
    df["Account Type"] = "Vendor"
    df["Bal. Account No."] = "20000"
    df["Posting Date"] = ""
    df["Shortcut Dimension 1 Code"] = "SAC"
    df["Gen. Bus. Posting Group"] = ""
    df["Gen. Prod. Posting Group"] = ""
    df["Bal. Account Type"] = "G/L Account"
    df["Document Date"] = ""
    df["Dimension Set ID"] = ""
    df["Comment"] = ""
    df["Invoice No."] = ""

    df_columns = ["Journal Template Name", "Journal Batch Name", "Line No.", "Account Type", "Account No.", "Posting Date", "Document Type", "Document No.", "Description", "Bal. Account No.", "Amount",
                  "Shortcut Dimension 1 Code", "Due Date", "Gen. Bus. Posting Group", "Gen. Prod. Posting Group", "Bal. Account Type", "Document Date", "External Document No.", "Dimension Set ID", "Comment", "Invoice No."]

    df = df.reindex(columns=df_columns)

    print(df)

    df.to_excel(onedrive_path + "Download Southware/openap_output_" +
                date_time.strftime("%m%d%y") + ".xlsx", index=False)
    df.to_csv(onedrive_path + "Download Southware/openap_output_" +
              date_time.strftime("%m%d%y") + ".csv", index=False)
    df.to_excel("files/to_bc_outputs/openap_output.xlsx", index=False)
    df.to_csv("files/to_bc_outputs/openap_output.csv", index=False)
    df.to_csv("files/to_bc_outputs/history/openap_output_" +
              date_time.strftime("%m%d%y_%H%M%S") + ".csv.gz", index=False, compression='gzip')


onedrive_path = "/Users/nadimtellezbarrera/Library/CloudStorage/OneDrive-EVI/07-SAC/Go Live Simluation 01.06.2023/4. Open AP/"
read_transform(onedrive_path)
