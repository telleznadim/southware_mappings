import pandas as pd
import math
import re
from datetime import datetime
from validate_email_string import validate_email_string


def defineIds(invoice_id, n_zeros):
    if (invoice_id != ""):
        print(invoice_id, type(invoice_id))
        return ("SAC" + str(invoice_id).zfill(n_zeros))
    else:
        return ("")


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


def customersNotFound(df):
    df_notfound = df[df["Customer No."] == "not_found"]
    df = df[df["Customer No."] != "not_found"]
    df_notfound.to_csv(
        "files/to_bc_outputs/ship_address_customers_notfound.csv", index=False)
    return (df)


def defineAddressCode(code):
    return (str(code).zfill(3))


def assignAddressCode(df):
    print("Setting mapping for column:  Code - Creating Ids ")
    df_unique = pd.DataFrame(df["Customer No."].unique(), columns=[
        "Customer No."]).sort_values(by=["Customer No."])
    df_final = pd.DataFrame()
    for index, row in df_unique.iterrows():
        df_customer_addresses = df.loc[df["Customer No."]
                                       == row["Customer No."]].copy()
        df_customer_addresses["Code"] = range(1, 1+len(df_customer_addresses))
        df_customer_addresses["Code"] = df_customer_addresses["Code"].apply(
            defineAddressCode)
        df_final = pd.concat([df_final, df_customer_addresses])

    return (df_final)


def read_transform(date_time, relations_dict):
    df = pd.read_excel(
        "files/from_southware/OPENPOL01-Southware-11-15-22.xlsx", dtype={"Purch For Order #": str}, keep_default_na=False)

    print(df)

    # df = df.sample(n=500, random_state=1)
    # Transform DATA

    # df["Ship-to Code"] = df["Ship-to Zip Code"].apply(cleanPhoneNo)
    # # df["Phone No."] = df["Phone Number"].apply(cleanPhoneNo)
    # # # df["Fax No."] = df["Fax No"].apply(cleanPhoneNo)
    # # df["Email"] = df["Email Address"].apply(cleanEmail)
    # # df["phone_sanitized"] = df["phone_sanitized"].apply(cleanPhoneNo)
    df["Document No."] = df["PO Number"].apply(defineIds, args=[7])

    df["Sales Order No."] = df["Purch For Order #"].apply(defineIds, args=[6])
    print(df["Sales Order No."])
    df['Line No.'] = (df["PO Line Number"]) * 1000

    # # get_mapping_columns = [
    # #     {"bc_column": "Sell-to Customer No.", "southware_column": "Customer Number",
    # #         "mapping_table": relations_dict["customer_ids"], "default_value": "not_found"},
    # #     {"bc_column": "Bill-to Customer No.", "southware_column": "Bill To Customer",
    # #      "mapping_table": relations_dict["customer_ids"], "default_value": "not_found"},
    # #     {"bc_column": "Payment Terms Code", "southware_column": "Terms Code",
    # #      "mapping_table": relations_dict["payment_terms_code"], "default_value": "ZZ-REVIEW"},
    # #     {"bc_column": "Salesperson Code", "southware_column": "Salesperson",
    # #      "mapping_table": relations_dict["salespeople"], "default_value": "not_found"},
    # #     {"bc_column": "Shipment Method Code", "southware_column": "Ship Via Code",
    # #      "mapping_table": relations_dict["shipment_method_code"], "default_value": "not_found"},
    # #     {"bc_column": "Location Code", "southware_column": "Location Number",
    # #      "mapping_table": relations_dict["location_code"], "default_value": "not_found"},
    # # ]

    # # for mapping_column in get_mapping_columns:
    # #     print("Setting mapping for column: ", mapping_column["bc_column"])
    # #     df[mapping_column["bc_column"]] = df[mapping_column["southware_column"]].apply(getMapping,
    # #                                                                                    args=(mapping_column["mapping_table"], mapping_column["default_value"],))

    df.rename(columns={"Stock Number": "No.", "Order Quantity": "Quantity",
              "Unit Cost": "Unit Cost ($)"}, inplace=True)

    # df["Document Type"] = "Order"

    df["Document Type"] = "Order"
    df["Buy-from Vendor No."] = ""
    df["Type"] = "Item"
    df["Posting Group"] = ""
    df["Expected Receipt Date"] = ""
    df["Description"] = ""
    df["Description 2"] = ""
    df["Unit of Measure"] = "EA"
    df["Direct Unit Cost"] = ""
    df["Tax %"] = ""
    df["Line Discount %"] = ""
    df["Allow Invoice Disc."] = ""
    df["Shortcut Dimension 1 Code"] = ""
    df["Shortcut Dimension 2 Code"] = ""
    df["Sales Order Line No."] = ""
    df["Gen. Bus. Posting Group"] = ""
    df["Tax Area Code"] = "STX_"
    df["Tax Liable"] = "true"
    df["Tax Group Code"] = "SURETAX"
    df["Dimension Set ID"] = ""

    # # # df["Tax Liable"] = "true"

    # # # # customer_columns = ["No.", "Name", "Address", "Address 2", "City", "State", "ZIP Code", "Contact", "Phone No.", "Salesperson Code", "Salesperson Number", "EORI Number", "Credit Limit ($)", "Blocked", "Shipment Method Code", "Ship Via Code", "Purchase Order Required", "Payment Terms Code", "Terms Code", "Legacy Addr 1", "Fax No.", "Customer Disc. Group", "Ship-to Code",
    # # # #                     "Legacy Note ID", "Email", "Country/Region Code", "Country Co          e", "Customer Since", "Legacy Addr 2", "Customer Vertical", "Vertical Code", "Customer Posting Group", "Gen. Bus. Posting Group", "Bill-to Customer No.", "Tax Area Code", "Tax Liable", "Territory Code", "Location Code", "National Account", "SureTax© Exemption Code", "Customer Number"]
    df_columns = ["Document Type", "Document No.", "Line No.", "Buy-from Vendor No.", "Type", "No.", "Location Code", "Posting Group", "Expected Receipt Date", "Description", "Description 2", "Unit of Measure", "Quantity", "Direct Unit Cost",
                  "Unit Cost ($)", "Tax %", "Line Discount %", "Allow Invoice Disc.", "Shortcut Dimension 1 Code", "Shortcut Dimension 2 Code", "Sales Order No.", "Sales Order Line No.", "Gen. Bus. Posting Group", "Tax Area Code", "Tax Liable", "Tax Group Code", "Dimension Set ID", ]

    df = df.reindex(columns=df_columns)
    # # df = customersNotFound(df)

    # # df = df.sort_values(by="Last Date Modified").drop_duplicates(
    # #     subset=['Customer No.', 'Address'], keep='last')

    # df = assignAddressCode(df)
    print(df)

    df.to_excel("files/to_bc_outputs/open_pos_lines_output.xlsx", index=False)
    df.to_csv("files/to_bc_outputs/open_pos_lines_output.csv", index=False)
    df.to_csv("files/to_bc_outputs/history/open_pos_lines_output_" +
              date_time.strftime("%m%d%y_%H%M%S") + ".csv.gz", index=False, compression='gzip')


def main():
    date_time = datetime.now()
    customer_ids = pd.read_csv("files/relations/customers/ids.csv")
    payment_terms_code = pd.read_csv(
        "files/relations/open_ar/payment_terms_code.csv")
    location_code = pd.read_csv(
        "files/relations/open_pos/location_code.csv")

    purchaser_code = pd.read_csv(
        "files/relations/open_pos/purchaser_code.csv")
    salespeople = pd.read_csv(
        "files/relations/customers/Salespeople-Southware.csv")
    shipment_method_code = pd.read_csv(
        "files/relations/customers/shipment_method_code.csv")

    # states = pd.read_csv("files/relations/customers/states.csv")
    # suretax = pd.read_csv("files/relations/ship_addresses/suretax.csv")
    # country_codes = pd.read_csv(
    #     "files/relations/customers/country_codes_2.csv")
    # shipment_method_code = pd.read_csv(
    #     "files/relations/customers/shipment_method_code.csv")
    relations_dict = {"customer_ids": customer_ids,
                      "payment_terms_code": payment_terms_code, "location_code": location_code, "purchaser_code": purchaser_code, "salespeople": salespeople, "shipment_method_code": shipment_method_code}
    # relations_dict = {}
    read_transform(date_time, relations_dict)


if __name__ == "__main__":
    main()
