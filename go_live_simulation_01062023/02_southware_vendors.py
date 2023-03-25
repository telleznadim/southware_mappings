import pandas as pd
import re
from datetime import datetime
from validate_email_string import validate_email_string

date_time = datetime.now()

vendor_ids = pd.read_csv("files/relations/vendors/ids.csv")
irs1099_code = pd.read_csv("files/relations/vendors/irs_1099_code.csv")
# print(customer_ids)
states = pd.read_csv("files/relations/customers/states.csv")
country_codes = pd.read_csv("files/relations/customers/country_codes_2.csv")
# cstvrt = pd.read_csv("files/relations/customers/CSTVRT.csv")
# salespeople = pd.read_csv(
#     "files/relations/customers/Salespeople-Southware.csv")
blocked = pd.read_csv(
    "files/relations/customers/blocked.csv")
# purchase_order_required = pd.read_csv(
#     "files/relations/customers/purchase_order_required.csv")
# shipment_method_code = pd.read_csv(
#     "files/relations/customers/shipment_method_code.csv")
payment_terms_code = pd.read_csv(
    "files/relations/vendors/payment_terms_code.csv")
# customer_vertical = pd.read_csv(
#     "files/relations/customers/customer_vertical.csv")
intercompany_vendors = pd.read_excel(
    "files/relations/vendors/intercompany_vendors.xlsx")


def defineIds(customer_id):
    return ("SAC-" + str(customer_id).zfill(5))


def getUniqueFromColumn(df, column_name):
    # print(df[column_name].unique())
    df_unique = pd.DataFrame(df[column_name].unique(), columns=[
        column_name]).sort_values(by=[column_name])
    # print(df_unique)

    column_name = column_name.replace("/", "_")
    df_unique.to_csv("files/unique/vendors_"+column_name +
                     "_unique_values.csv", index=False)
    # for item in df.unique():
    #     print(item)


def cleanEmail(email_string):
    if (validate_email_string(str(email_string)) == True):
        return (email_string)
    else:
        return ("")


def setBlocked(isInactive):
    blocked = ""
    if (isInactive == "Y"):
        blocked = "All"

    return blocked


def cleanPhoneNo(phone_no):
    phone_no = str(phone_no)
    return (re.sub('[^0-9 *+*-]', '', phone_no))


def modifyIdsRelation(df_new, column_name):
    # print(customer_ids)
    print("Printing DF_NEW from modifyIdsRelation")
    # print(df_new)
    columns = ["No.", column_name]
    df_new = df_new.reindex(columns=columns)
    df_new.rename(columns={"No.": "new_value",
                  column_name: "old_value"}, inplace=True)
    df_customers_ids_final = pd.concat([vendor_ids, df_new])
    # print(df_customers_ids_final)
    df_customers_ids_final.to_csv(
        "files/relations/vendors/ids.csv", index=False)


def createFinalDfAddNo(df, last_id):
    print(" ------ createFinalDfAddNo -----")
    df_old = df[df["No."] != ""]
    df_new = df[df["No."] == ""]
    print(df_old)
    print(df_new)
    # print(print(df_new["Customer Number"]))
    if (len(df_new) > 0):
        df_new["No."] = range((last_id), last_id + len(df_new))
        df_new["No."] = df_new["No."].apply(defineIds)
        modifyIdsRelation(df_new, "Vendor Number")
    # print("df_new", df_new)
    df_final = pd.concat([df_old, df_new])
    return (df_final)


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


def read_transform(onedrive_path):
    df = pd.read_excel(
        onedrive_path + "Complete Master/VENDOR01-SAC-01-06-2023.xlsx")
    print(df)
    # df = df.sample(n=1000, random_state=1)
    # Transform DATA

    df["ZIP Code"] = df["Zip Code"].apply(cleanPhoneNo)
    df["Phone No."] = df["Phone Number"].apply(cleanPhoneNo)
    df["Fax No."] = df["Fax No"].apply(cleanPhoneNo)
    df["Email"] = df["EMail Address"].apply(cleanEmail)
    # df["phone_sanitized"] = df["phone_sanitized"].apply(cleanPhoneNo)

    get_mapping_columns = [
        {"bc_column": "No.", "southware_column": "Vendor Number",
            "mapping_table": vendor_ids, "default_value": ""},
        {"bc_column": "IRS 1099 Code", "southware_column": "1099 Vendor ? [Y/N]",
            "mapping_table": irs1099_code, "default_value": ""},
        # {"bc_column": "Blocked", "southware_column": "Vendor Inactive?",
        #     "mapping_table": blocked, "default_value": ""},
        # {"bc_column": "Purchase Order Required", "southware_column": "Requires P/O ? Y/N",
        #     "mapping_table": purchase_order_required, "default_value": ""},
        # {"bc_column": "Shipment Method Code", "southware_column": "Ship Via Code",
        #     "mapping_table": shipment_method_code, "default_value": "not_found"},
        {"bc_column": "Payment Terms Code", "southware_column": "Terms Code",
            "mapping_table": payment_terms_code, "default_value": "ZZ-REVIEW"},
        # {"bc_column": "Customer Vertical", "southware_column": "Vertical Code",
        #     "mapping_table": customer_vertical, "default_value": "ZZ-REVIEW"},
        {"bc_column": "State", "southware_column": "State",
            "mapping_table": states, "default_value": ""},
        {"bc_column": "Country/Region Code", "southware_column": "Country Code",
            "mapping_table": country_codes, "default_value": ""},
        {"bc_column": "Vendor Posting Group", "southware_column": "Vendor Number",
         "mapping_table": intercompany_vendors, "default_value": "STD"},
        {"bc_column": "Gen. Bus. Posting Group", "southware_column": "Vendor Number",
         "mapping_table": intercompany_vendors, "default_value": "STD"}
    ]

    for mapping_column in get_mapping_columns:
        print("Setting mapping for column: ", mapping_column["bc_column"])
        df[mapping_column["bc_column"]] = df[mapping_column["southware_column"]].apply(getMapping,
                                                                                       args=(mapping_column["mapping_table"], mapping_column["default_value"],))
    # df["No."] = df["Customer Number"].apply(
    #     getMapping, args=(customer_ids, "",))
    # print(df["No."])
    # df["Blocked"] = df["Customer Inactive?"].apply(setBlocked)
    # df["Salesperson Code"] = df["Salesperson Number"].apply(getMapping,
    #                                                         args=(salespeople, "ZZ-REVIEW",))
    # df["Blocked"] = df["Credit Rating"].apply(getMapping,
    #                                           args=(blocked, "",))
    # df["Purchase Order Required"] = df["Requires P/O ? Y/N"].apply(getMapping,
    #                                                                args=(purchase_order_required, "",))

    # df["Shipment Method Code"] = df["Ship Via Code"].apply(getMapping,
    #                                                        args=(shipment_method_code, "not_found",))
    # df["Payment Terms Code"] = df["Terms Code"].apply(getMapping,
    #                                                   args=(payment_terms_code, "ZZ-REVIEW",))
    # df["Customer Vertical"] = df["Vertical Code"].apply(getMapping,
    #                                                     args=(customer_vertical, "ZZ-REVIEW",))
    # df["State"] = df["State"].apply(getMapping,
    #                                 args=(states, "",))
    # df["Country/Region Code"] = df["Country Code"].apply(getMapping,
    #                                                      args=(country_codes, "",))
    df = df.sort_values(by=['Vendor Posting Group', "Vendor Number"])
    df.rename(columns={"Vendor Name": "Name", "Address Line 1": "Address", "Address Line 2": "Address 2", "Contact Name": "Contact",
              "1099 Fed ID Number": "Federal ID No.", "Date Created": "Vendor Since", "Notes Key": "Legacy Addr 1", "Auto Dist Acct# 1": "Legacy Addr 2"}, inplace=True)

    df["Search Name"] = ""
    df["Blocked"] = ""
    df["Name 2"] = ""
    # df["Vendor Posting Group"] = "STD"
    # df["Gen. Bus. Posting Group"] = "STD"
    df["Our Account No."] = ""
    df["Shipment Method Code"] = ""
    df["Shipping Agent Code"] = ""
    df["Payment Method Code"] = ""
    df["Last Date Modified"] = ""
    df["Tax Area Code"] = "STX_"
    df["Tax Liable"] = "true"
    df["Brand"] = ""

    # getColumns(df)

    # customer_columns = ["No.", "Name", "Address", "Address 2", "City", "State", "ZIP Code", "Contact", "Phone No.", "Salesperson Code", "Salesperson Number", "EORI Number", "Credit Limit ($)", "Blocked", "Shipment Method Code", "Ship Via Code", "Purchase Order Required", "Payment Terms Code", "Terms Code", "Legacy Addr 1", "Fax No.", "Customer Disc. Group", "Ship-to Code",
    #                     "Legacy Note ID", "Email", "Country/Region Code", "Country Code", "Customer Since", "Legacy Addr 2", "Customer Vertical", "Vertical Code", "Customer Posting Group", "Gen. Bus. Posting Group", "Bill-to Customer No.", "Tax Area Code", "Tax Liable", "Territory Code", "Location Code", "National Account", "SureTax© Exemption Code", "Customer Number"]
    customer_columns = ["No.", "Name", "Address", "Address 2", "City", "State", "ZIP Code", "Payment Terms Code", "Contact", "Phone No.", "IRS 1099 Code", "Federal ID No.", "Blocked", "Fax No.", "Email", "Country/Region Code", "Vendor Since", "Legacy Addr 1",
                        "Legacy Addr 2", "Search Name", "Name 2", "Vendor Posting Group", "Gen. Bus. Posting Group", "Our Account No.", "Shipment Method Code", "Shipping Agent Code", "Payment Method Code", "Last Date Modified", "Tax Area Code", "Tax Liable", "Brand", "Vendor Number"]

    df = df.reindex(columns=customer_columns)

    df = createFinalDfAddNo(df, len(vendor_ids) +
                            60000).sort_values(by="No.")
    print(df)

    # getUniqueFromColumn(df, "State")
    # getUniqueFromColumn(df, "Salesperson Code")
    # getUniqueFromColumn(df, "EORI Number")
    # getUniqueFromColumn(df, "Blocked")  # HOLD - ALL, REST BLANK
    # getUniqueFromColumn(df, "Shipment Method Code")
    # getUniqueFromColumn(df, "Purchase Order Required")  # TRUE OR FALSE
    # getUniqueFromColumn(df, "Payment Terms Code")
    # getUniqueFromColumn(df, "IRS 1099 Code")
    # getUniqueFromColumn(df, "Blocked")
    # getUniqueFromColumn(df, "Customer Vertical")

    df.to_excel(onedrive_path + "Download Southware/vendors_output" +
                date_time.strftime("%m%d%y") + ".xlsx", index=False)
    df.to_csv(onedrive_path + "Download Southware/vendors_output" +
              date_time.strftime("%m%d%y") + ".csv", index=False)
    df.to_excel("files/to_bc_outputsvendors_output.xlsx", index=False)
    df.to_csv("files/to_bc_outputs/vendors_output.csv", index=False)
    df.to_csv("files/to_bc_outputs/history/vendors_output_" +
              date_time.strftime("%m%d%y_%H%M%S") + ".csv.gz", index=False, compression='gzip')


onedrive_path = "/Users/nadimtellezbarrera/Library/CloudStorage/OneDrive-EVI/07-SAC/Go Live Simluation 01.06.2023/Vendor/"
read_transform(onedrive_path)
