import pandas as pd
import re
from datetime import datetime
from validate_email_string import validate_email_string

date_time = datetime.now()

customer_ids = pd.read_csv("files/relations/customers/ids.csv")
# print(customer_ids)
states = pd.read_csv("files/relations/customers/states.csv")
country_codes = pd.read_csv("files/relations/customers/country_codes_2.csv")
cstvrt = pd.read_csv("files/relations/customers/CSTVRT.csv")
salespeople = pd.read_csv(
    "files/relations/customers/Salespeople-Southware.csv")
blocked = pd.read_csv(
    "files/relations/customers/blocked.csv")
purchase_order_required = pd.read_csv(
    "files/relations/customers/purchase_order_required.csv")
shipment_method_code = pd.read_csv(
    "files/relations/customers/shipment_method_code.csv")
payment_terms_code = pd.read_csv(
    "files/relations/customers/payment_terms_code.csv")
customer_vertical = pd.read_csv(
    "files/relations/customers/customer_vertical.csv")
intercompany_customers = pd.read_excel(
    "files/relations/customers/intercompany_customers.xlsx")


def defineIds(customer_id):
    return ("SAC-" + str(customer_id).zfill(5))


def getColumns(df):
    print(df.columns)
    for col in df.columns:
        print(col)


def modifyIdsRelation(df_new):
    # print(customer_ids)
    print("Printing DF_NEW from modifyIdsRelation")
    # print(df_new)
    columns = ["No.", "Customer Number"]
    df_new = df_new.reindex(columns=columns)
    df_new.rename(columns={"No.": "new_value",
                  "Customer Number": "old_value"}, inplace=True)
    df_customers_ids_final = pd.concat([customer_ids, df_new])
    # print(df_customers_ids_final)

    df_customers_ids_final.to_csv(
        "files/relations/customers/ids.csv", index=False)


def createFinalDfAddNo(df, last_id):
    print(" ------ createFinalDfAddNo -----")
    df_old = df[df["No."] != ""]
    df_new = df[df["No."] == ""]
    # print(print(df_new["Customer Number"]))
    if (len(df_new) > 0):
        df_new["No."] = range((last_id), last_id + len(df_new))
        df_new["No."] = df_new["No."].apply(defineIds)
        modifyIdsRelation(df_new)
    # print("df_new", df_new)
    df_final = pd.concat([df_old, df_new])
    return (df_final)


def setBlocked(isInactive):
    blocked = ""
    if (isInactive == "Y"):
        blocked = "All"

    return blocked


def cleanPhoneNo(phone_no):
    phone_no = str(phone_no)
    return (re.sub('[^0-9 *+*-]', '', phone_no))


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


# def getMapping(old_value, relation_csv, default_value):
#     new_value = old_value
#     if (new_value != "False"):
#         find_new = relation_csv.loc[relation_csv["old_value"]
#                                     == new_value]['new_value']
#         # print(find_new)
#         if (not find_new.empty):
#             new_value = relation_csv.loc[relation_csv["old_value"]
#                                          == new_value]['new_value'].values[0]
#     # elif (new_value == ""):
#     #     new_value = ""
#     else:
#         new_value = default_value
#     return (new_value)

def cleanEmail(email_string):
    if (validate_email_string(str(email_string)) == True):
        return (email_string)
    else:
        return ("")


def getUniqueFromColumn(df, column_name):
    # print(df[column_name].unique())
    df_unique = pd.DataFrame(df[column_name].unique(), columns=[
        column_name]).sort_values(by=[column_name])
    # print(df_unique)

    column_name = column_name.replace("/", "_")
    df_unique.to_csv("files/unique/"+column_name +
                     "_unique_values.csv", index=False)
    # for item in df.unique():
    #     print(item)


def set_addr1_from_addr2(df):
    df = pd.read_csv("files/to_bc_outputs/customers_output.csv")
    print(df)
    # df.loc[df['Address'].isna(), 'Address 2'] = df['Address']
    indices = df.loc[df['Address'].isna()].index
    for index in indices:
        # print(index)
        # print(df.iloc[index])
        df.iloc[index, df.columns.get_loc(
            'Address')] = df.iloc[index]["Address 2"]
        df.iloc[index, df.columns.get_loc(
            'Address 2')] = ""

    print(df)
    print(df.loc[df['Address'].isna()])
    return (df)


def read_transform(onedrive_path):
    df = pd.read_excel(
        onedrive_path + "1. Complete Master/CUSTOMER01-SAC-01-27-2023-316pm.xlsx")
    print(df)
    # df = df.sample(n=1000, random_state=1)
    # Transform DATA
    customer_ids.to_csv(
        "files/relations/customers/ids/bu_ids_" +
        date_time.strftime("%m%d%y_%H%M%S") + ".csv", index=False)

    df["ZIP Code"] = df["Zip Code"].apply(cleanPhoneNo)
    df["Phone No."] = df["Phone Number"].apply(cleanPhoneNo)
    df["Fax No."] = df["Fax No"].apply(cleanPhoneNo)
    df["Email"] = df["EMail Address"].apply(cleanEmail)
    df["Customer Disc. Group"] = df["Discount Percent"]*100
    # df["phone_sanitized"] = df["phone_sanitized"].apply(cleanPhoneNo)

    get_mapping_columns = [
        {"bc_column": "No.", "southware_column": "Customer Number",
            "mapping_table": customer_ids, "default_value": ""},
        {"bc_column": "Salesperson Code", "southware_column": "Salesperson Number",
            "mapping_table": salespeople, "default_value": "ZZ-REVIEW"},
        {"bc_column": "Blocked", "southware_column": "Credit Rating",
            "mapping_table": blocked, "default_value": ""},
        {"bc_column": "Purchase Order Required", "southware_column": "Requires P/O ? Y/N",
            "mapping_table": purchase_order_required, "default_value": ""},
        {"bc_column": "Shipment Method Code", "southware_column": "Ship Via Code",
            "mapping_table": shipment_method_code, "default_value": ""},
        {"bc_column": "Payment Terms Code", "southware_column": "Terms Code",
            "mapping_table": payment_terms_code, "default_value": "ZZ-REVIEW"},
        {"bc_column": "Customer Vertical", "southware_column": "Vertical Code",
            "mapping_table": customer_vertical, "default_value": "ZZ-UND"},
        {"bc_column": "State", "southware_column": "State",
            "mapping_table": states, "default_value": ""},
        {"bc_column": "Country/Region Code", "southware_column": "Country Code",
            "mapping_table": country_codes, "default_value": ""},
        {"bc_column": "Customer Posting Group", "southware_column": "Customer Number",
         "mapping_table": intercompany_customers, "default_value": "STD"},
        {"bc_column": "Gen. Bus. Posting Group", "southware_column": "Customer Number",
         "mapping_table": intercompany_customers, "default_value": "STD"}
    ]

    for mapping_column in get_mapping_columns:
        print("Setting mapping for column: ", mapping_column["bc_column"])
        df[mapping_column["bc_column"]] = df[mapping_column["southware_column"]].apply(getMapping,
                                                                                       args=(mapping_column["mapping_table"], mapping_column["default_value"],))
    df = df.sort_values(by=['Customer Posting Group', "Customer Number"])
    df.rename(columns={"Customer Name": "Name", "Address Line 1": "Address", "Address Line 2": "Address 2", "Credit Limit": "Credit Limit ($)",
              "Address Line 3": "Legacy Addr 1", "Tax #": "Legacy Note ID", "Date Created": "Customer Since"}, inplace=True)

    df["Location Code"] = ""
    df["Tax Area Code"] = "STX_"
    df["Territory Code"] = ""
    df["EORI Number"] = "EQP"
    df["Tax Liable"] = "True"
    df["Legacy Addr 2"] = ""
    df["Ship-to Code"] = ""
    df["National Account"] = ""
    df["SureTax© Exemption Code"] = ""

    customer_columns = ["No.", "Name", "Address", "Address 2", "City", "State", "ZIP Code", "Contact", "Phone No.", "Salesperson Code", "EORI Number", "Credit Limit ($)", "Blocked", "Shipment Method Code", "Purchase Order Required", "Payment Terms Code", "Legacy Addr 1", "Fax No.", "Customer Disc. Group", "Ship-to Code",
                        "Legacy Note ID", "Email", "Country/Region Code", "Customer Since", "Legacy Addr 2", "Customer Vertical", "Customer Posting Group", "Gen. Bus. Posting Group", "Bill-to Customer No.", "Tax Area Code", "Tax Liable", "Territory Code", "Location Code", "National Account", "SureTax© Exemption Code", "Customer Number"]

    print(df)
    df = df.reindex(columns=customer_columns)

    df = createFinalDfAddNo(df, len(customer_ids) +
                            50000).sort_values(by="No.")
    df["Bill-to Customer No."] = df["No."]
    print('Moving Addr2 to Addre1 when Addre1 es BLANK')
    df = set_addr1_from_addr2(df)
    print(df)

    df.to_excel(onedrive_path + "2. Download Southware/customers_output" +
                date_time.strftime("%m%d%y") + ".xlsx", index=False)
    df.to_csv(onedrive_path + "2. Download Southware/customers_output" +
              date_time.strftime("%m%d%y") + ".csv", index=False)
    df.to_excel("files/to_bc_outputs/customers_output.xlsx", index=False)
    df.to_csv("files/to_bc_outputs/customers_output.csv", index=False)
    df.to_csv("files/to_bc_outputs/history/customers_output_" +
              date_time.strftime("%m%d%y_%H%M%S") + ".csv.gz", index=False, compression='gzip')


def main():
    onedrive_path = "/Users/nadimtellezbarrera/Library/CloudStorage/OneDrive-EVI/07-SAC/Go Live/1. Customer/"
    read_transform(onedrive_path)


if __name__ == "__main__":
    main()
