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


def read_get_lines_from_headers(onedrive_path, date_time, sos_headers_filename):

    df = pd.read_excel(f'{onedrive_path}Complete Master/{sos_headers_filename}.xlsx', dtype={
                       "Misc Charge Code 1": str, "Misc Charge Code 2": str, "Misc Charge Code 3": str, "Misc Charge Code 4": str})
    # print(df)

    # Filtering misc Charge Code not Null
    df_lines_1 = df[df["Misc Charge Code 1"].notnull(
    )][["Order Number", "Misc Charge Code 1", "Misc Charge Amount 1"]]
    df_lines_2 = df[df["Misc Charge Code 2"].notnull(
    )][["Order Number", "Misc Charge Code 2", "Misc Charge Amount 2"]]
    df_lines_3 = df[df["Misc Charge Code 3"].notnull(
    )][["Order Number", "Misc Charge Code 3", "Misc Charge Amount 3"]]
    df_lines_4 = df[df["Misc Charge Code 4"].notnull(
    )][["Order Number", "Misc Charge Code 4", "Misc Charge Amount 4"]]

    df_lines_1.rename(columns={"Misc Charge Code 1": "Stock # Ordered",
                      "Misc Charge Amount 1": "Unit Price"}, inplace=True)
    df_lines_2.rename(columns={"Misc Charge Code 2": "Stock # Ordered",
                      "Misc Charge Amount 2": "Unit Price"}, inplace=True)
    df_lines_3.rename(columns={"Misc Charge Code 3": "Stock # Ordered",
                      "Misc Charge Amount 3": "Unit Price"}, inplace=True)
    df_lines_4.rename(columns={"Misc Charge Code 4": "Stock # Ordered",
                      "Misc Charge Amount 4": "Unit Price"}, inplace=True)

    df_lines = pd.concat(
        [df_lines_1, df_lines_2, df_lines_3, df_lines_4], ignore_index=True)
    df_lines["Quantity To Ship"] = 1
    # Filtering Unit Price not BLANK
    df_lines = df_lines[df_lines["Unit Price"].notna()]
    print(df_lines)
    df_lines.to_csv("files/tmp/so_lines_from_headers.csv", index=False)
    df_lines.to_csv(
        f'{onedrive_path}Download Southware/so_lines_from_headers_{date_time.strftime("%m%d%y")}.csv', index=False)

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


def read_transform_lines(date_time, relations_dict, onedrive_path, sos_lines_filename, sos_headers_filename):
    df = pd.read_excel(
        f'{onedrive_path}Complete Master/{sos_lines_filename}.xlsx', dtype={"Stock # Ordered": str, "Order Number": str, }, skiprows=2)

    df["Order_Line_Number"] = df["Order Number"].astype(
        str) + "_" + df["Line Number"].astype(str)
    print(df)

    # df3 = pd.read_excel(
    #     "files/relations/open_sos/SecondAdjustmentSalesOrder.xlsx")
    # df3["Order_Line_Number"] = df3["Order Number"].astype(
    #     str) + "_" + df3["Line Number"].astype(str)
    # print(df3)

    # df = df[df["Order_Line_Number"].isin(
    #     df3["Order_Line_Number"])]
    # print(df)

    # df2 = read_get_lines_from_headers(
    #     onedrive_path, date_time, sos_headers_filename)
    # df = pd.concat([df, df2], ignore_index=True)
    # print(df)

    df["Document No."] = df["Order Number"].apply(defineIds)
    df['Line No.'] = (df["Line Number"]) * 1000

    get_mapping_columns = [
        # {"bc_column": "header_exists", "source_column": "Document No.",
        #     "mapping_table": relations_dict["open_sos_headers_output"], "default_value": False, "old_value_column_name": "No.", "new_value_column_name": "No."},
        {"bc_column": "Document Type", "source_column": "Document No.",
            "mapping_table": relations_dict["open_sos_headers_output"], "default_value": "not_found_in_header", "old_value_column_name": "No.", "new_value_column_name": "Document Type"},
        {"bc_column": "Sell-to Customer No.", "source_column": "Document No.",
            "mapping_table": relations_dict["open_sos_headers_output"], "default_value": "not_found_in_header", "old_value_column_name": "No.", "new_value_column_name": "Sell-to Customer No."},
        {"bc_column": "Gen. Bus. Posting Group", "source_column": "Document No.",
         "mapping_table": relations_dict["open_sos_headers_output"], "default_value": "not_found_in_header", "old_value_column_name": "No.", "new_value_column_name": "Gen. Bus. Posting Group"},
        {"bc_column": "SLEPRS Code (Dimension)", "source_column": "Document No.",
         "mapping_table": relations_dict["open_sos_headers_output"], "default_value": "not_found_in_header", "old_value_column_name": "No.", "new_value_column_name": "SLEPRS Code (Dimension)"},
        {"bc_column": "Order type", "source_column": "Document No.",
         "mapping_table": relations_dict["open_sos_headers_output"], "default_value": "not_found_in_header", "old_value_column_name": "No.", "new_value_column_name": "Order Type"},
        {"bc_column": "Opptype Code (Dimension)", "source_column": "Location Number",
         "mapping_table": relations_dict["location_code"], "default_value": "", "old_value_column_name": "old_value", "new_value_column_name": "new_value"},
        {"bc_column": "No.", "source_column": "Stock # Ordered",
         "mapping_table": relations_dict["items"], "default_value": "not_found", "old_value_column_name": "SAC Stock Number", "new_value_column_name": "BC New Item No."},
    ]

    for mapping_column in get_mapping_columns:
        print("Setting mapping for column: ", mapping_column["bc_column"])
        df[mapping_column["bc_column"]] = df[mapping_column["source_column"]].apply(getMappingColumnNames,
                                                                                    args=(mapping_column["mapping_table"], mapping_column["default_value"], mapping_column["old_value_column_name"], mapping_column["new_value_column_name"]))

    df["Quantity"] = df["Quantity To Ship"]
    df.rename(columns={"Quantity": "Quantity", "Description 1": "Description",
              "Unit Price": "Unit Price", "Unit Cost": "Unit Cost ($)", "Discount Percent": "Line Discount %"}, inplace=True)

    df["Type"] = "Item"
    df["Posting Group"] = ""
    df["Shipment Date"] = ""
    df["Unit of Measure"] = "EA"
    df["Tax %"] = ""
    df["Amount"] = ""
    df["Allow Invoice Disc."] = ""
    df["Shortcut Dimension 1 Code"] = "SAC"
    df["Shortcut Dimension 2 Code"] = ""
    df["Purchase Order No."] = ""
    df["Purch. Order Line No."] = ""
    df["Tax Area Code"] = "STX_"
    df["Purchasing Code"] = ""
    df["Planned Delivery Date"] = ""
    df["Planned Shipment Date"] = ""
    df["Unit Cost for Margin"] = ""
    df["Margin %"] = ""
    df["Planned Receipt Date"] = ""
    df["Purchase Order No.2"] = ""
    df["EVI BU Code (Dimension)"] = "SAC"

    df_columns = ["Document Type", "Document No.", "Line No.", "Sell-to Customer No.", "Type", "No.", "Posting Group", "Shipment Date", "Description", "Unit of Measure", "Quantity To Ship", "Quantity Ordered", "Unit Price",
                  "Unit Cost ($)", "Tax %", "Line Discount %", "Amount", "Allow Invoice Disc.", "Shortcut Dimension 1 Code", "Shortcut Dimension 2 Code", "Purchase Order No.", "Purch. Order Line No.", "Gen. Bus. Posting Group", "Tax Area Code", "Purchasing Code", "Planned Delivery Date", "Planned Shipment Date", "Unit Cost for Margin", "Margin %", "Planned Receipt Date", "Purchase Order No.2", "EVI BU Code (Dimension)", "Opptype Code (Dimension)", "SLEPRS Code (Dimension)", "Stock # Ordered", "header_exists", "Order type"]

    df = df.reindex(columns=df_columns)
    # df = build_sales_lines_no(df)

    print(df)

    df.to_excel(
        f'{onedrive_path}Download Southware/{sos_lines_filename}_lines_output_{date_time.strftime("%m%d%y")}.xlsx', index=False)
    # df.to_csv(onedrive_path + "Download Southware/open_sos_lines_output_" +
    #           date_time.strftime("%m%d%y") + ".csv", index=False)
    df.to_excel("files/to_bc_outputs/open_sos_lines_output.xlsx", index=False)
    df.to_csv("files/to_bc_outputs/open_sos_lines_output.csv", index=False)
    df.to_csv("files/to_bc_outputs/history/open_sos_lines_output_" +
              date_time.strftime("%m%d%y_%H%M%S") + ".csv.gz", index=False, compression='gzip')


def delete_headers_if_not_exist_lines(date_time, onedrive_path, sos_headers_filename):
    df_so_lines = pd.read_csv(
        f'files/to_bc_outputs/open_sos_lines_output.csv', keep_default_na=False)
    df_so_headers = pd.read_csv(
        f'files/to_bc_outputs/open_sos_headers_output.csv', keep_default_na=False)
    print(df_so_headers)
    print(df_so_lines["Document No."])
    print(df_so_headers["No."])
    df_so_headers = df_so_headers[df_so_headers["No."].isin(
        df_so_lines["Document No."])]
    print(df_so_headers)

    df_so_headers.to_excel(
        f'{onedrive_path}Download Southware/{sos_headers_filename}_headers_output_{date_time.strftime("%m%d%y")}.xlsx', index=False)
    # df_so_headers.to_csv(onedrive_path + "Download Southware/open_sos_headers_output_" +
    #                      date_time.strftime("%m%d%y") + ".csv", index=False)
    df_so_headers.to_excel(
        "files/to_bc_outputs/open_sos_headers_output.xlsx", index=False)
    df_so_headers.to_csv(
        "files/to_bc_outputs/open_sos_headers_output.csv", index=False)
    df_so_headers.to_csv("files/to_bc_outputs/history/open_sos_headers_output_" +
                         date_time.strftime("%m%d%y_%H%M%S") + ".csv.gz", index=False, compression='gzip')


def main():
    date_time = datetime.now()
    sos_lines_filename = "SLSORDRH01-SAC-02-01-2023-223PM-REVIEW-GM1"
    sos_headers_filename = "SLSORDRH01-SAC-01-30-2023-939am"
    onedrive_path = "/Users/nadimtellezbarrera/Library/CloudStorage/OneDrive-EVI/07-SAC/Go Live/7. Sales Order Header-Lines/"
    location_code = pd.read_csv(
        "files/relations/open_sos/location_code_2.csv")
    open_sos_headers_output = pd.read_csv(
        "files/to_bc_outputs/open_sos_headers_output.csv")
    srv_charge = pd.read_csv('files/relations/open_sos/srv_charge.csv')
    items = pd.read_excel(
        f'/Users/nadimtellezbarrera/Library/CloudStorage/OneDrive-EVI/07-SAC/Go Live/9. Items/4. Upload BC/SAC Items Simple Map 01.27.2023_AM_V3.xlsx')

    print("Concatening Items and Srv Charge")
    items = pd.concat([items, srv_charge], ignore_index=True)

    relations_dict = {
        "open_sos_headers_output": open_sos_headers_output, "location_code": location_code, "items": items}

    read_transform_lines(date_time, relations_dict,
                         onedrive_path, sos_lines_filename, sos_headers_filename)
    delete_headers_if_not_exist_lines(
        date_time, onedrive_path, sos_headers_filename)


if __name__ == "__main__":
    main()
