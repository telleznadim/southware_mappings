import pandas as pd
import re
from datetime import datetime
from validate_email_string import validate_email_string


def defineIds(invoice_id):
    return ("SAC" + str(invoice_id).zfill(7))


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


def read_transform(date_time, relations_dict, onedrive_path):
    df = pd.read_excel(
        onedrive_path + "Complete Master/PRCHORDH01-SAC-01-06-2023.xlsx")
    print(df)

    df = df[df["Status"] == "O"]
    print(df)
    print(df["Buyer Name"])

    # df = df.sample(n=500, random_state=1)
    # Transform DATA

    # df["ZIP Code"] = df["Ship-to Zip Code"].apply(cleanPhoneNo)
    # df["Phone No."] = df["Phone Number"].apply(cleanPhoneNo)
    # # df["Fax No."] = df["Fax No"].apply(cleanPhoneNo)
    # df["Email"] = df["Email Address"].apply(cleanEmail)
    # df["phone_sanitized"] = df["phone_sanitized"].apply(cleanPhoneNo)
    df["No."] = df["PO Number"].apply(defineIds)

    get_mapping_columns = [
        {"bc_column": "Buy-from Vendor No.", "southware_column": "Vendor Number",
            "mapping_table": relations_dict["vendor_ids"], "default_value": "not_found"},
        {"bc_column": "Payment Terms Code", "southware_column": "Payment Terms Code",
         "mapping_table": relations_dict["payment_terms_code"], "default_value": "ZZ-REVIEW"},
        {"bc_column": "Location Code", "southware_column": "Location",
         "mapping_table": relations_dict["location_code"], "default_value": "not_found"},
        {"bc_column": "Purchaser Code", "southware_column": "Buyer Code",
         "mapping_table": relations_dict["purchaser_code"], "default_value": "ZZ-REVIEW"},
        {"bc_column": "Shipment Method Code", "southware_column": "Ship Via Description",
         "mapping_table": relations_dict["shipment_method_code"], "default_value": ""},
        {"bc_column": "Assigned User ID", "southware_column": "Buyer Name",
         "mapping_table": relations_dict["user_ids"], "default_value": ""}
    ]

    for mapping_column in get_mapping_columns:
        print("Setting mapping for column: ", mapping_column["bc_column"])
        df[mapping_column["bc_column"]] = df[mapping_column["southware_column"]].apply(getMapping,
                                                                                       args=(mapping_column["mapping_table"], mapping_column["default_value"],))

    df.rename(columns={"Requisition Number": "Your Reference", "Ship To Name": "Ship-to Name", "Ship To Address 1": "Ship-to Address", "Ship To Address 2": "Ship-to Address 2", "Ship To City": "Ship-to City", "Po Contact Name": "Ship-to Contact",
              "Date Ordered": "Order Date", "Ship To Zip": "Ship-to ZIP Code", "Ship To State": "Ship-to State", "Date Created": "Document Date"}, inplace=True)

    df["Document Type"] = "Order"
    df["Posting Date"] = ""
    df["Due Date"] = ""
    df["Payment Discount %"] = ""
    df["Shortcut Dimension 1 Code"] = "SAC"
    df["Shortcut Dimension 2 Code"] = ""
    df["Vendor Posting Group"] = "STD"
    df["Gen. Bus. Posting Group"] = "STD"
    df["Tax Area Code"] = "STX_"
    df["Tax Liable"] = "True"
    df["Dimension Set ID"] = ""
    # df["Assigned User ID"] = ""
    df["IRS 1099 Code"] = ""
    df["EVI BU"] = "SAC"
    df["Payment Method Code"] = "CHECK"

    # df["Tax Liable"] = "true"

    # # customer_columns = ["No.", "Name", "Address", "Address 2", "City", "State", "ZIP Code", "Contact", "Phone No.", "Salesperson Code", "Salesperson Number", "EORI Number", "Credit Limit ($)", "Blocked", "Shipment Method Code", "Ship Via Code", "Purchase Order Required", "Payment Terms Code", "Terms Code", "Legacy Addr 1", "Fax No.", "Customer Disc. Group", "Ship-to Code",
    # #                     "Legacy Note ID", "Email", "Country/Region Code", "Country Co          e", "Customer Since", "Legacy Addr 2", "Customer Vertical", "Vertical Code", "Customer Posting Group", "Gen. Bus. Posting Group", "Bill-to Customer No.", "Tax Area Code", "Tax Liable", "Territory Code", "Location Code", "National Account", "SureTax© Exemption Code", "Customer Number"]
    df_columns = ["Document Type",	"No.",	"Buy-from Vendor No.",	"Your Reference",	"Ship-to Name",	"Ship-to Address",	"Ship-to Address 2",	"Ship-to City",	"Ship-to Contact",	"Order Date",	"Posting Date",	"Payment Terms Code",	"Due Date",	"Payment Discount %",	"Shipment Method Code",	"Location Code",
                  "Shortcut Dimension 1 Code",	"Shortcut Dimension 2 Code",	"Vendor Posting Group",	"Purchaser Code",	"Gen. Bus. Posting Group",	"Ship-to ZIP Code",	"Ship-to State",	"Document Date",	"Payment Method Code",	"Tax Area Code", "Tax Liable",	"Dimension Set ID",	"Assigned User ID",	"IRS 1099 Code",	"EVI BU"]

    df = df.reindex(columns=df_columns)
    # df = customersNotFound(df)

    # df = df.sort_values(by="Last Date Modified").drop_duplicates(
    #     subset=['Customer No.', 'Address'], keep='last')

    # df = assignAddressCode(df)
    print(df)
    df.to_excel(onedrive_path + "Download Southware/open_pos_headers_output" +
                date_time.strftime("%m%d%y") + ".xlsx", index=False)
    df.to_csv(onedrive_path + "Download Southware/open_pos_headers_output" +
              date_time.strftime("%m%d%y") + ".csv", index=False)

    df.to_excel("files/to_bc_outputs/open_pos_headers_output.xlsx", index=False)
    df.to_csv("files/to_bc_outputs/open_pos_headers_output.csv", index=False)
    df.to_csv("files/to_bc_outputs/history/open_pos_headers_output_" +
              date_time.strftime("%m%d%y_%H%M%S") + ".csv.gz", index=False, compression='gzip')


def main():
    date_time = datetime.now()
    onedrive_path = "/Users/nadimtellezbarrera/Library/CloudStorage/OneDrive-EVI/07-SAC/Go Live Simluation 01.06.2023/6. Purchase Order Header-Lines/"
    vendor_ids = pd.read_csv("files/relations/vendors/ids.csv")
    payment_terms_code = pd.read_csv(
        "files/relations/vendors/payment_terms_code.csv")
    location_code = pd.read_csv(
        "files/relations/open_pos/location_code.csv")

    purchaser_code = pd.read_csv(
        "files/relations/open_pos/purchaser_code.csv")
    shipment_method_code = pd.read_csv(
        "files/relations/open_pos/shipment_method_code.csv")
    user_ids = pd.read_csv("files/relations/open_pos/user_id.csv")

    # states = pd.read_csv("files/relations/customers/states.csv")
    # suretax = pd.read_csv("files/relations/ship_addresses/suretax.csv")
    # country_codes = pd.read_csv(
    #     "files/relations/customers/country_codes_2.csv")
    # shipment_method_code = pd.read_csv(
    #     "files/relations/customers/shipment_method_code.csv")
    relations_dict = {"vendor_ids": vendor_ids, "payment_terms_code": payment_terms_code, "location_code": location_code,
                      "purchaser_code": purchaser_code, "shipment_method_code": shipment_method_code, "user_ids": user_ids}
    # relations_dict = {}
    read_transform(date_time, relations_dict, onedrive_path)


if __name__ == "__main__":
    main()
