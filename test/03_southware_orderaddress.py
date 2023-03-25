import pandas as pd
import re
from datetime import datetime
from validate_email_string import validate_email_string

date_time = datetime.now()

vendor_ids = pd.read_csv("files/relations/vendors/ids.csv")
states = pd.read_csv("files/relations/customers/states.csv")


def cleanEmail(email_string):
    if (validate_email_string(str(email_string)) == True):
        return (email_string)
    else:
        return ("")


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


def read_transform():
    df = pd.read_excel(
        "files/from_southware/Vendor01-ORDADDRSS-Southware-11-15-2022.xlsx")
    print(df)
    # df = df.sample(n=1000, random_state=1)
    # Transform DATA

    df["ZIP Code"] = df["Pay Zip"].apply(cleanPhoneNo)
    # df["Phone No."] = df["Phone Number"].apply(cleanPhoneNo)
    df["Fax No."] = df["Fax No"].apply(cleanPhoneNo)
    df["Email"] = df["EMail Address"].apply(cleanEmail)
    # # df["phone_sanitized"] = df["phone_sanitized"].apply(cleanPhoneNo)

    get_mapping_columns = [
        {"bc_column": "Vendor No.", "southware_column": "Vendor Number",
            "mapping_table": vendor_ids, "default_value": ""},
        {"bc_column": "State", "southware_column": "Pay State",
         "mapping_table": states, "default_value": ""},
    ]

    for mapping_column in get_mapping_columns:
        print("Setting mapping for column: ", mapping_column["bc_column"])
        df[mapping_column["bc_column"]] = df[mapping_column["southware_column"]].apply(getMapping,
                                                                                       args=(mapping_column["mapping_table"], mapping_column["default_value"],))
    # # df["No."] = df["Customer Number"].apply(
    # #     getMapping, args=(customer_ids, "",))
    # # print(df["No."])
    # # df["Blocked"] = df["Customer Inactive?"].apply(setBlocked)
    # # df["Salesperson Code"] = df["Salesperson Number"].apply(getMapping,
    # #                                                         args=(salespeople, "ZZ-REVIEW",))
    # # df["Blocked"] = df["Credit Rating"].apply(getMapping,
    # #                                           args=(blocked, "",))
    # # df["Purchase Order Required"] = df["Requires P/O ? Y/N"].apply(getMapping,
    # #                                                                args=(purchase_order_required, "",))

    # # df["Shipment Method Code"] = df["Ship Via Code"].apply(getMapping,
    # #                                                        args=(shipment_method_code, "not_found",))
    # # df["Payment Terms Code"] = df["Terms Code"].apply(getMapping,
    # #                                                   args=(payment_terms_code, "ZZ-REVIEW",))
    # # df["Customer Vertical"] = df["Vertical Code"].apply(getMapping,
    # #                                                     args=(customer_vertical, "ZZ-REVIEW",))
    # # df["State"] = df["State"].apply(getMapping,
    # #                                 args=(states, "",))
    # # df["Country/Region Code"] = df["Country Code"].apply(getMapping,
    # #                                                      args=(country_codes, "",))

    df.rename(columns={"Pay Name": "Name", "Pay Address Line 1": "Address", "Pay Address Line 2": "Address 2", "Pay City": "City",
              "Pay Country Code": "Country/Region Code", "Contact Name": "Contact"}, inplace=True)

    df["Code"] = "CHECK"
    # df["Blocked"] = ""
    # df["Name 2"] = ""
    # df["Vendor Posting Group"] = "STD"
    # df["Gen. Bus. Posting Group"] = "STD"
    # df["Our Account No."] = ""
    # df["Shipment Method Code"] = ""
    # df["Shipping Agent Code"] = ""
    # df["Payment Method Code"] = ""
    # df["Last Date Modified"] = ""
    # df["Tax Area Code"] = "STX_"
    # df["Tax Liable"] = "true"
    # df["Brand"] = ""

    # # getColumns(df)

    # # customer_columns = ["No.", "Name", "Address", "Address 2", "City", "State", "ZIP Code", "Contact", "Phone No.", "Salesperson Code", "Salesperson Number", "EORI Number", "Credit Limit ($)", "Blocked", "Shipment Method Code", "Ship Via Code", "Purchase Order Required", "Payment Terms Code", "Terms Code", "Legacy Addr 1", "Fax No.", "Customer Disc. Group", "Ship-to Code",
    # #                     "Legacy Note ID", "Email", "Country/Region Code", "Country Code", "Customer Since", "Legacy Addr 2", "Customer Vertical", "Vertical Code", "Customer Posting Group", "Gen. Bus. Posting Group", "Bill-to Customer No.", "Tax Area Code", "Tax Liable", "Territory Code", "Location Code", "National Account", "SureTax© Exemption Code", "Customer Number"]
    df_columns = ["Vendor No.", "Code", "Name", "Address", "Address 2", "City", "State",
                  "ZIP Code", "Country/Region Code", "Contact", "Phone No.", "Fax No.", "Email"]

    df = df.reindex(columns=df_columns)

    # df = createFinalDfAddNo(df, len(vendor_ids) +
    #                         60000).sort_values(by="No.")
    print(df)

    # # getUniqueFromColumn(df, "State")
    # # getUniqueFromColumn(df, "Salesperson Code")
    # # getUniqueFromColumn(df, "EORI Number")
    # # getUniqueFromColumn(df, "Blocked")  # HOLD - ALL, REST BLANK
    # # getUniqueFromColumn(df, "Shipment Method Code")
    # # getUniqueFromColumn(df, "Purchase Order Required")  # TRUE OR FALSE
    # # getUniqueFromColumn(df, "Payment Terms Code")
    # # getUniqueFromColumn(df, "IRS 1099 Code")
    # # getUniqueFromColumn(df, "Blocked")
    # # getUniqueFromColumn(df, "Customer Vertical")

    df.to_excel("files/to_bc_outputs/order_address_output.xlsx", index=False)
    df.to_csv("files/to_bc_outputs/order_address_output.csv", index=False)
    df.to_csv("files/to_bc_outputs/history/order_address_output_" +
              date_time.strftime("%m%d%y_%H%M%S") + ".csv.gz", index=False, compression='gzip')


read_transform()
