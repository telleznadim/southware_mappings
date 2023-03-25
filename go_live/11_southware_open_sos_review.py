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


def read_transform(date_time, relations_dict, onedrive_path):
    df = pd.read_excel(
        onedrive_path + "Complete Master/SLSORD-Headers-N-01-30-2023-253PM.xlsx")
    print(df)
    # Order Type = R
    # Order Type = D
    # Order Type = Q
    df = df[((df["Order Date"] >= "01/01/2022") &
             (df["Order Date"] <= "01/27/2023"))]
    df = df[(df["Order Type"] == "R") | (
        df["Order Type"] == "D") | (df["Order Type"] == "Q")]
    print(f'SOs after filter...\n', df)
    df['Salesperson'] = df['Salesperson'].astype(str)
    print(df)

    # df = df.sample(n=500, random_state=1)
    # Transform DATA

    df["Ship-to ZIP Code"] = df["Ship-to Zip Code"].apply(cleanPhoneNo)
    # df["Phone No."] = df["Phone Number"].apply(cleanPhoneNo)
    # # df["Fax No."] = df["Fax No"].apply(cleanPhoneNo)
    # df["Email"] = df["Email Address"].apply(cleanEmail)
    # df["phone_sanitized"] = df["phone_sanitized"].apply(cleanPhoneNo)
    df["No."] = df["Order Number"].apply(defineIds)

    get_mapping_columns = [
        {"bc_column": "Sell-to Customer No.", "source_column": "Customer Number",
            "mapping_table": relations_dict["customer_ids"], "default_value": "not_found"},
        {"bc_column": "Bill-to Customer No.", "source_column": "Bill To Customer",
         "mapping_table": relations_dict["customer_ids"], "default_value": "not_found"},
        {"bc_column": "Payment Terms Code", "source_column": "Terms Code",
         "mapping_table": relations_dict["payment_terms_code"], "default_value": "ZZ-REVIEW"},
        {"bc_column": "Salesperson Code", "source_column": "Salesperson",
         "mapping_table": relations_dict["salespeople"], "default_value": "not_found"},
        {"bc_column": "Shipment Method Code", "source_column": "Ship Via Code",
         "mapping_table": relations_dict["shipment_method_code"], "default_value": "not_found"},
        {"bc_column": "Location Code", "source_column": "Location Number",
         "mapping_table": relations_dict["location_code"], "default_value": "not_found"},
        {"bc_column": "Ship-to State", "source_column": "Ship-to State",
            "mapping_table": relations_dict["states"], "default_value": ""},
        {"bc_column": "Opptype Code (Dimension)", "source_column": "Location Code",
         "mapping_table": relations_dict["oportunity_type"], "default_value": ""},
        {"bc_column": "Document Type", "source_column": "Order Type",
         "mapping_table": relations_dict["document_type"], "default_value": "Order"},
        {"bc_column": "Gen. Bus. Posting Group", "source_column": "Customer Number",
         "mapping_table": relations_dict["intercompany_customers"], "default_value": "STD"},
        {"bc_column": "Customer Posting Group", "source_column": "Customer Number",
         "mapping_table": relations_dict["intercompany_customers"], "default_value": "STD"},


    ]

    for mapping_column in get_mapping_columns:
        print("Setting mapping for column: ", mapping_column["bc_column"])
        df[mapping_column["bc_column"]] = df[mapping_column["source_column"]].apply(getMapping,
                                                                                    args=(mapping_column["mapping_table"], mapping_column["default_value"],))

    df.rename(columns={"Order Date": "Order Date",
              "Order Date": "Document Date", "Comment": "Work Description", "Ship-to Address 1": "Ship-to Address"}, inplace=True)

    df["Ship-to Code"] = ""
    df["Ship-to Country/Region Code"] = ""
    # df["Document Type"] = "Order"
    df["Your Reference"] = ""
    df["Posting Date"] = ""
    df["Shipment Date"] = ""
    df["Shortcut Dimension 1 Code"] = "SAC"
    # df["Customer Posting Group"] = ""
    # df["Gen. Bus. Posting Group"] = ""
    df["External Document No."] = ""
    df["Payment Method Code"] = ""
    df["Tax Area Code"] = "STX_"
    df["Tax Liable"] = "TRUE"
    df["EVI BU Code (Dimension)"] = "SAC"
    df["SLEPRS Code (Dimension)"] = df["Salesperson Code"]

    # # df["Tax Liable"] = "true"

    # # # customer_columns = ["No.", "Name", "Address", "Address 2", "City", "State", "ZIP Code", "Contact", "Phone No.", "Salesperson Code", "Salesperson Number", "EORI Number", "Credit Limit ($)", "Blocked", "Shipment Method Code", "Ship Via Code", "Purchase Order Required", "Payment Terms Code", "Terms Code", "Legacy Addr 1", "Fax No.", "Customer Disc. Group", "Ship-to Code",
    # # #                     "Legacy Note ID", "Email", "Country/Region Code", "Country Co          e", "Customer Since", "Legacy Addr 2", "Customer Vertical", "Vertical Code", "Customer Posting Group", "Gen. Bus. Posting Group", "Bill-to Customer No.", "Tax Area Code", "Tax Liable", "Territory Code", "Location Code", "National Account", "SureTax© Exemption Code", "Customer Number"]
    df_columns = ["Document Type", "No.", "Sell-to Customer No.", "Bill-to Customer No.", "Your Reference", "Ship-to Code", "Ship-to Address", "Ship-to Address 2", "Ship-to City", "Ship-to State", "Ship-to ZIP Code", "Ship-to Country/Region Code", "Order Date", "Posting Date", "Shipment Date", "Payment Terms Code", "Shipment Method Code", "Location Code",
                  "Shortcut Dimension 1 Code", "Customer Posting Group", "Salesperson Code", "Gen. Bus. Posting Group", "Document Date", "External Document No.", "Payment Method Code", "Tax Area Code", "Tax Liable", "Work Description", "EVI BU Code (Dimension)", "Opptype Code (Dimension)", "SLEPRS Code (Dimension)", "Order Type"]

    df = df.reindex(columns=df_columns)
    # # df = customersNotFound(df)

    # # df = df.sort_values(by="Last Date Modified").drop_duplicates(
    # #     subset=['Customer No.', 'Address'], keep='last')

    # df = assignAddressCode(df)
    print(df)

    df.to_excel(onedrive_path + "Download Southware/open_sos_headers_output_review_" +
                date_time.strftime("%m%d%y") + ".xlsx", index=False)
    # df.to_csv(onedrive_path + "Download Southware/open_sos_headers_output_" +
    #           date_time.strftime("%m%d%y") + ".csv", index=False)
    df.to_excel("files/to_bc_outputs/open_sos_headers_output.xlsx", index=False)
    df.to_csv("files/to_bc_outputs/open_sos_headers_output.csv", index=False)
    df.to_csv("files/to_bc_outputs/history/open_sos_headers_output_" +
              date_time.strftime("%m%d%y_%H%M%S") + ".csv.gz", index=False, compression='gzip')


def main():
    date_time = datetime.now()
    onedrive_path = "/Users/nadimtellezbarrera/Library/CloudStorage/OneDrive-EVI/07-SAC/Go Live/7. Sales Order Header-Lines/"
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
    states = pd.read_csv("files/relations/customers/states.csv")
    oportunity_type = pd.read_csv(
        "files/relations/open_Sos/oportunity_type.csv")
    document_type = pd.read_csv(
        "files/relations/open_Sos/document_type.csv")
    intercompany_customers = pd.read_excel(
        "files/relations/customers/intercompany_customers.xlsx")

    # states = pd.read_csv("files/relations/customers/states.csv")
    # suretax = pd.read_csv("files/relations/ship_addresses/suretax.csv")
    # country_codes = pd.read_csv(
    #     "files/relations/customers/country_codes_2.csv")
    # shipment_method_code = pd.read_csv(
    #     "files/relations/customers/shipment_method_code.csv")
    relations_dict = {"customer_ids": customer_ids, "payment_terms_code": payment_terms_code, "location_code": location_code, "purchaser_code": purchaser_code,
                      "salespeople": salespeople, "shipment_method_code": shipment_method_code, "states": states, "oportunity_type": oportunity_type, "document_type": document_type, "intercompany_customers": intercompany_customers}
    read_transform(date_time, relations_dict, onedrive_path)


if __name__ == "__main__":
    main()
