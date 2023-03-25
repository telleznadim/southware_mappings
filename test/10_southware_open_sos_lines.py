import pandas as pd
import re
from datetime import datetime
from validate_email_string import validate_email_string


def defineIds(invoice_id):
    return ("SAC" + str(invoice_id).zfill(6))


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


def read_get_lines_from_headers():
    df = pd.read_excel(
        "files/from_southware/OPENSLSOHDUMP-12-01-2022.xlsx")
    print(df)

    df_lines_1 = df[df["Misc Charge Code 1"].notnull(
    )][["Order Number", "Misc Charge Code 1", "Misc Charge Amount 1"]]
    df_lines_2 = df[df["Misc Charge Code 2"].notnull(
    )][["Order Number", "Misc Charge Code 2", "Misc Charge Amount 2"]]
    df_lines_3 = df[df["Misc Charge Code 3"].notnull(
    )][["Order Number", "Misc Charge Code 3", "Misc Charge Amount 3"]]
    df_lines_4 = df[df["Misc Charge Code 4"].notnull(
    )][["Order Number", "Misc Charge Code 4", "Misc Charge Amount 4"]]

    df_lines_1.rename(columns={"Misc Charge Code 1": "Item ID (stock/Svc)",
                      "Misc Charge Amount 1": "Unit Price"}, inplace=True)
    df_lines_2.rename(columns={"Misc Charge Code 2": "Item ID (stock/Svc)",
                      "Misc Charge Amount 2": "Unit Price"}, inplace=True)
    df_lines_3.rename(columns={"Misc Charge Code 3": "Item ID (stock/Svc)",
                      "Misc Charge Amount 3": "Unit Price"}, inplace=True)
    df_lines_4.rename(columns={"Misc Charge Code 4": "Item ID (stock/Svc)",
                      "Misc Charge Amount 4": "Unit Price"}, inplace=True)

    # print(df_lines_1)
    # print(df_lines_2)
    # print(df_lines_3)
    # print(df_lines_4)
    # for index, row in df.iterrows():
    #     print(row['Order Number'], row["Misc Charge Code 1"].isnull())

    df_lines = pd.concat(
        [df_lines_1, df_lines_2, df_lines_3, df_lines_4], ignore_index=True)
    df_lines["Quantity Ordered"] = 1
    print(df_lines)
    return (df_lines)


def set_line_no(df_order):
    # print("----- Setting Lines ----")
    df_order_no_lines = df_order[df_order["Line No."].isnull()].copy()
    df_order_lines = df_order[df_order["Line No."].notnull()].copy()

    if (not df_order_no_lines.empty):
        # print(df_order_no_lines)
        last_line = 10000
        if (df_order["Line No."].max() > 0):
            last_line = int(df_order["Line No."].max()) + 10000
        # print(df_order_no_lines)
        # print(len(df_order_no_lines))
        # print(range(
        #     last_line, last_line + ((len(df_order_no_lines)) * 10000), 10000))
        df_order_no_lines["Line No."] = range(
            last_line, last_line + ((len(df_order_no_lines)) * 10000), 10000)
        # for index, row in df_order_no_lines.iterrows():
        #     row['Line No.']=last_line
        #     last_line=+ 10000

        # print(df_order_lines)
        # print(df_order_no_lines)
        df_order = pd.concat([df_order_lines, df_order_no_lines])

    return (df_order)


def build_sales_lines_no(df):
    document_nos = df["Document No."].unique()
    df_final = pd.DataFrame()
    for document_no in document_nos:
        # df[df["Document No."] == document_no]
        # df_order = df[df["Document No."] == document_no]
        df_order = set_line_no(df[df["Document No."] == document_no].copy())
        # print(df_order)
        df_final = pd.concat([df_final, df_order])
        # print(df_order)

        # print(df_order["Line No."].max())
        # print(df_order[df_order["Line No."].isnull()])
    print(df_final)
    return (df_final)


def set_line_no_easy(df):

    document_nos = df["Document No."].unique()
    df_final = pd.DataFrame()
    for document_no in document_nos:
        # print(df.loc[df["Document No."] == document_no]["Line No."])
        # df.loc[df["Document No."] == document_no]["Line No."] = 9878768768.0
        # print(df_order)
        df_order = df[df["Document No."] == document_no].copy()
        # df_order.loc[:, ("Line No.")] = 12039848
        # df_order["Line No."] = 12314
        df_order["Line No."] = range(10000, (10000*len(df_order))+1, 10000)
        # print(df_order)
        df_final = pd.concat([df_final, df_order])

    print(df_final)
    return (df_final)


def read_transform_lines(date_time, relations_dict):
    df = pd.read_excel(
        "files/from_southware/SLSLN01-Soutware-BC-11-15-2022.xlsx")
    df2 = read_get_lines_from_headers()
    print(df)
    df = pd.concat([df, df2], ignore_index=True)

    # df = df.sample(n=500, random_state=1)
    # Transform DATA

    # df["Ship-to Code"] = df["Ship-to Zip Code"].apply(cleanPhoneNo)
    # # df["Phone No."] = df["Phone Number"].apply(cleanPhoneNo)
    # # # df["Fax No."] = df["Fax No"].apply(cleanPhoneNo)
    # # df["Email"] = df["Email Address"].apply(cleanEmail)
    # # df["phone_sanitized"] = df["phone_sanitized"].apply(cleanPhoneNo)
    df["Document No."] = df["Order Number"].apply(defineIds)
    df['Line No.'] = (df["Line Number"]) * 1000

    # get_mapping_columns = [
    #     {"bc_column": "Sell-to Customer No.", "southware_column": "Customer Number",
    #         "mapping_table": relations_dict["customer_ids"], "default_value": "not_found"},
    #     {"bc_column": "Bill-to Customer No.", "southware_column": "Bill To Customer",
    #      "mapping_table": relations_dict["customer_ids"], "default_value": "not_found"},
    #     {"bc_column": "Payment Terms Code", "southware_column": "Terms Code",
    #      "mapping_table": relations_dict["payment_terms_code"], "default_value": "ZZ-REVIEW"},
    #     {"bc_column": "Salesperson Code", "southware_column": "Salesperson",
    #      "mapping_table": relations_dict["salespeople"], "default_value": "not_found"},
    #     {"bc_column": "Shipment Method Code", "southware_column": "Ship Via Code",
    #      "mapping_table": relations_dict["shipment_method_code"], "default_value": "not_found"},
    #     {"bc_column": "Location Code", "southware_column": "Location Number",
    #      "mapping_table": relations_dict["location_code"], "default_value": "not_found"},
    # ]

    # for mapping_column in get_mapping_columns:
    #     print("Setting mapping for column: ", mapping_column["bc_column"])
    #     df[mapping_column["bc_column"]] = df[mapping_column["southware_column"]].apply(getMapping,
    #                                                                                    args=(mapping_column["mapping_table"], mapping_column["default_value"],))

    df.rename(columns={"Item ID (stock/Svc)": "No.", "Quantity Ordered": "Quantity",
              "Unit Price": "Unit Price", "Unit Cost": "Unit Cost ($)", "Discount Percent": "Line Discount %"}, inplace=True)

    df["Document Type"] = "Order"

    df["Sell-to Customer No."] = ""
    df["Type"] = "Item"
    df["Posting Group"] = ""
    df["Shipment Date"] = ""
    df["Description"] = ""
    df["Unit of Measure"] = "EA"
    df["Tax %"] = ""
    df["Amount"] = ""
    df["Allow Invoice Disc."] = ""
    df["Shortcut Dimension 1 Code"] = "SAC"
    df["Shortcut Dimension 2 Code"] = ""
    df["Purchase Order No."] = ""
    df["Purch. Order Line No."] = ""
    df["Gen. Bus. Posting Group"] = ""
    df["Tax Area Code"] = ""
    df["Purchasing Code"] = ""
    df["Planned Delivery Date"] = ""
    df["Planned Shipment Date"] = ""
    df["Unit Cost for Margin"] = ""
    df["Margin %"] = ""
    df["Planned Receipt Date"] = ""
    df["Purchase Order No.2"] = ""
    df["EVI BU Code (Dimension)"] = "SAC"
    df["Opptype Code (Dimension)"] = ""
    df["SLEPRS Code (Dimension)"] = ""

    # # df["Tax Liable"] = "true"

    # # # customer_columns = ["No.", "Name", "Address", "Address 2", "City", "State", "ZIP Code", "Contact", "Phone No.", "Salesperson Code", "Salesperson Number", "EORI Number", "Credit Limit ($)", "Blocked", "Shipment Method Code", "Ship Via Code", "Purchase Order Required", "Payment Terms Code", "Terms Code", "Legacy Addr 1", "Fax No.", "Customer Disc. Group", "Ship-to Code",
    # # #                     "Legacy Note ID", "Email", "Country/Region Code", "Country Co          e", "Customer Since", "Legacy Addr 2", "Customer Vertical", "Vertical Code", "Customer Posting Group", "Gen. Bus. Posting Group", "Bill-to Customer No.", "Tax Area Code", "Tax Liable", "Territory Code", "Location Code", "National Account", "SureTax© Exemption Code", "Customer Number"]
    df_columns = ["Document Type", "Document No.", "Line No.", "Sell-to Customer No.", "Type", "No.", "Posting Group", "Shipment Date", "Description", "Unit of Measure", "Quantity", "Unit Price",
                  "Unit Cost ($)", "Tax %", "Line Discount %", "Amount", "Allow Invoice Disc.", "Shortcut Dimension 1 Code", "Shortcut Dimension 2 Code", "Purchase Order No.", "Purch. Order Line No.", "Gen. Bus. Posting Group", "Tax Area Code", "Purchasing Code", "Planned Delivery Date", "Planned Shipment Date", "Unit Cost for Margin", "Margin %", "Planned Receipt Date", "Purchase Order No.2", "EVI BU Code (Dimension)", "Opptype Code (Dimension)", "SLEPRS Code (Dimension)"]

    df = df.reindex(columns=df_columns)
    # # df = customersNotFound(df)

    # # df = df.sort_values(by="Last Date Modified").drop_duplicates(
    # #     subset=['Customer No.', 'Address'], keep='last')

    # df = assignAddressCode(df)
    print(df)
    df = build_sales_lines_no(df)
    # set_line_no_easy(df)
    df.to_excel("files/to_bc_outputs/open_sos_lines_output.xlsx", index=False)
    df.to_csv("files/to_bc_outputs/open_sos_lines_output.csv", index=False)
    df.to_csv("files/to_bc_outputs/history/open_sos_lines_output_" +
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
    read_transform_lines(date_time, relations_dict)


if __name__ == "__main__":
    main()
