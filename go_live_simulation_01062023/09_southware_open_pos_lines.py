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


def getMappingColumnNames(old_value, relation_csv, default_value, old_value_column_name, new_value_column_name):
    new_value = old_value
    # print("starting_value", new_value)
    if (new_value != ""):
        find_new = relation_csv.loc[relation_csv[old_value_column_name]
                                    == new_value][new_value_column_name]
        # print("empty: ", find_new.empty)
        if (not find_new.empty):
            new_value = relation_csv.loc[relation_csv[old_value_column_name]
                                         == new_value][new_value_column_name].values[0]
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


def read_transform(date_time, relations_dict, onedrive_path):
    df = pd.read_excel(
        f'{onedrive_path}Complete Master/PRCHORDL01-SAC-01-06-2023.xlsx', dtype={"Purch For Order #": str}, keep_default_na=False)

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
    df["Quantity"] = df["Order Quantity"] - df["Received Todate Qty"]

    get_mapping_columns = [
        {"bc_column": "header_exists", "source_column": "Document No.",
            "mapping_table": relations_dict["open_pos_headers_output"], "default_value": False, "old_value_column_name": "No.", "new_value_column_name": "No."},
        {"bc_column": "SLEPRS Code (Dimension)", "source_column": "Document No.",
         "mapping_table": relations_dict["open_pos_headers_output"], "default_value": "", "old_value_column_name": "No.", "new_value_column_name": "Purchaser Code"},
        {"bc_column": "Location Code", "source_column": "Document No.",
         "mapping_table": relations_dict["open_pos_headers_output"], "default_value": "", "old_value_column_name": "No.", "new_value_column_name": "Location Code"},
        {"bc_column": "Buy-from Vendor No.", "source_column": "Document No.",
         "mapping_table": relations_dict["open_pos_headers_output"], "default_value": "", "old_value_column_name": "No.", "new_value_column_name": "Buy-from Vendor No."},
        {"bc_column": "No.", "source_column": "Stock Number",
         "mapping_table": relations_dict["items"], "default_value": "", "old_value_column_name": "SAC Stock Number", "new_value_column_name": "BC New Item No."},
    ]

    for mapping_column in get_mapping_columns:
        print("Setting mapping for column: ", mapping_column["bc_column"])
        df[mapping_column["bc_column"]] = df[mapping_column["source_column"]].apply(getMappingColumnNames,
                                                                                    args=(mapping_column["mapping_table"], mapping_column["default_value"], mapping_column["old_value_column_name"], mapping_column["new_value_column_name"]))

    df.rename(columns={
              "Unit Cost": "Unit Cost ($)", "Descrip": "Description"}, inplace=True)

    # df["Document Type"] = "Order"
    df["Direct Unit Cost"] = df["Unit Cost ($)"]
    df["Document Type"] = "Order"
    # df["Buy-from Vendor No."] = ""
    df["Type"] = "Item"
    df["Posting Group"] = ""
    df["Expected Receipt Date"] = ""
    # df["Description"] = ""
    df["Description 2"] = ""
    df["Unit of Measure"] = "EA"

    df["Tax %"] = ""
    df["Line Discount %"] = ""
    df["Allow Invoice Disc."] = ""
    df["Shortcut Dimension 1 Code"] = "SAC"
    df["Shortcut Dimension 2 Code"] = ""
    df["Sales Order Line No."] = ""
    df["Gen. Bus. Posting Group"] = ""
    df["Tax Area Code"] = "STX_"
    df["Tax Liable"] = "true"
    df["Tax Group Code"] = "SURETAX"
    df["Dimension Set ID"] = ""
    df["EVI BU Code (Dimension)"] = "SAC"
    # df["SLEPRS Code (Dimension)"] = "Purchaser Code"

    # # # df["Tax Liable"] = "true"
    print(df)
    print(f'Deleting PO Lines with 0 or less Quantity and with no header..')
    df = df[(df["Quantity"] > 0) & df["header_exists"] != False]
    print(df)

    # # # # customer_columns = ["No.", "Name", "Address", "Address 2", "City", "State", "ZIP Code", "Contact", "Phone No.", "Salesperson Code", "Salesperson Number", "EORI Number", "Credit Limit ($)", "Blocked", "Shipment Method Code", "Ship Via Code", "Purchase Order Required", "Payment Terms Code", "Terms Code", "Legacy Addr 1", "Fax No.", "Customer Disc. Group", "Ship-to Code",
    # # # #                     "Legacy Note ID", "Email", "Country/Region Code", "Country Co          e", "Customer Since", "Legacy Addr 2", "Customer Vertical", "Vertical Code", "Customer Posting Group", "Gen. Bus. Posting Group", "Bill-to Customer No.", "Tax Area Code", "Tax Liable", "Territory Code", "Location Code", "National Account", "SureTax© Exemption Code", "Customer Number"]
    df_columns = ["Document Type", "Document No.", "Line No.", "Buy-from Vendor No.", "Type", "No.", "Location Code", "Posting Group", "Expected Receipt Date", "Description", "Description 2", "Unit of Measure", "Quantity", "Direct Unit Cost",
                  "Unit Cost ($)", "Tax %", "Line Discount %", "Allow Invoice Disc.", "Shortcut Dimension 1 Code", "Shortcut Dimension 2 Code", "Sales Order No.", "Sales Order Line No.", "Gen. Bus. Posting Group", "Tax Area Code", "Tax Liable", "Tax Group Code", "Dimension Set ID", "EVI BU Code (Dimension)", "SLEPRS Code (Dimension)", "Stock Number"]

    df = df.reindex(columns=df_columns)
    # # df = customersNotFound(df)

    # # df = df.sort_values(by="Last Date Modified").drop_duplicates(
    # #     subset=['Customer No.', 'Address'], keep='last')

    # df = assignAddressCode(df)
    print(df)

    df.to_excel(onedrive_path + "Download Southware/open_pos_lines_output" +
                date_time.strftime("%m%d%y") + ".xlsx", index=False)
    df.to_csv(onedrive_path + "Download Southware/open_pos_lines_output" +
              date_time.strftime("%m%d%y") + ".csv", index=False)
    df.to_excel("files/to_bc_outputs/open_pos_lines_output.xlsx", index=False)
    df.to_csv("files/to_bc_outputs/open_pos_lines_output.csv", index=False)
    df.to_csv("files/to_bc_outputs/history/open_pos_lines_output_" +
              date_time.strftime("%m%d%y_%H%M%S") + ".csv.gz", index=False, compression='gzip')


def delete_headers_if_not_exist_lines(date_time, onedrive_path):
    df_po_lines = pd.read_csv(
        f'files/to_bc_outputs/open_pos_lines_output.csv', keep_default_na=False)
    df_po_headers = pd.read_csv(
        f'files/to_bc_outputs/open_pos_headers_output.csv', keep_default_na=False)

    print(df_po_lines["Document No."])
    print(df_po_headers["No."])
    df_po_headers = df_po_headers[df_po_headers["No."].isin(
        df_po_lines["Document No."])]
    print(df_po_headers)
    df_po_headers.to_excel(onedrive_path + "Download Southware/open_pos_headers_output" +
                           date_time.strftime("%m%d%y") + ".xlsx", index=False)
    df_po_headers.to_csv(onedrive_path + "Download Southware/open_pos_headers_output" +
                         date_time.strftime("%m%d%y") + ".csv", index=False)
    df_po_headers.to_excel(
        "files/to_bc_outputs/open_pos_headers_output.xlsx", index=False)
    df_po_headers.to_csv(
        "files/to_bc_outputs/open_pos_headers_output.csv", index=False)
    df_po_headers.to_csv("files/to_bc_outputs/history/open_pos_headers_output_" +
                         date_time.strftime("%m%d%y_%H%M%S") + ".csv.gz", index=False, compression='gzip')


def main():

    date_time = datetime.now()
    onedrive_path = "/Users/nadimtellezbarrera/Library/CloudStorage/OneDrive-EVI/07-SAC/Go Live Simluation 01.06.2023/6. Purchase Order Header-Lines/"
    open_pos_headers_output = pd.read_csv(
        "files/to_bc_outputs/open_pos_headers_output.csv")

    items = pd.read_excel(
        "files/relations/open_sos/SAC Items Simple Map 01.15.2023_SL.xlsx")
    print(items)

    relations_dict = {
        "open_pos_headers_output": open_pos_headers_output, "items": items}
    # relations_dict = {}
    read_transform(date_time, relations_dict, onedrive_path)
    delete_headers_if_not_exist_lines(date_time, onedrive_path)


if __name__ == "__main__":
    main()
