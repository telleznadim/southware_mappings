import pandas as pd
import re
from datetime import datetime
from validate_email_string import validate_email_string


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
        "files/from_southware/SHIPTOADDSS-Southware-11-15-2022.xlsx")
    print(df)
    # df = df.sample(n=500, random_state=1)
    # Transform DATA

    df["ZIP Code"] = df["Ship-to Zip Code"].apply(cleanPhoneNo)
    df["Phone No."] = df["Phone Number"].apply(cleanPhoneNo)
    # df["Fax No."] = df["Fax No"].apply(cleanPhoneNo)
    df["Email"] = df["Email Address"].apply(cleanEmail)
    df["Code"] = df["Ship-to Number"]
    # # # df["phone_sanitized"] = df["phone_sanitized"].apply(cleanPhoneNo)

    get_mapping_columns = [
        {"bc_column": "Customer No.", "southware_column": "Customer Number",
            "mapping_table": relations_dict["customer_ids"], "default_value": "not_found"},
        {"bc_column": "State", "southware_column": "Ship-to State",
         "mapping_table": relations_dict["states"], "default_value": ""},
        {"bc_column": "Shipment Method Code", "southware_column": "Ship To Ship Via",
         "mapping_table": relations_dict["shipment_method_code"], "default_value": ""},
        {"bc_column": "Country/Region Code", "southware_column": "Country Code",
         "mapping_table": relations_dict["country_codes"], "default_value": ""},
        {"bc_column": "SureTax© Exemption Code", "southware_column": "Ship-to Sales Tax Cd",
         "mapping_table": relations_dict["suretax"], "default_value": ""},
    ]

    for mapping_column in get_mapping_columns:
        print("Setting mapping for column: ", mapping_column["bc_column"])
        df[mapping_column["bc_column"]] = df[mapping_column["southware_column"]].apply(getMapping,
                                                                                       args=(mapping_column["mapping_table"], mapping_column["default_value"],))

    df.rename(columns={"Ship-to Name": "Name", "Ship-to Address 1": "Address", "Ship-to Address 2": "Address 2", "Ship-to City": "City",
              "Ship To Address 3": "Legacy Addr 1", "Contact": "Contact", "Date Modified": "Last Date Modified", "Notes Key": "Legacy Addr 2"}, inplace=True)

    df["Tax Area Code"] = "STX_"
    df["Tax Liable"] = "true"

    # # customer_columns = ["No.", "Name", "Address", "Address 2", "City", "State", "ZIP Code", "Contact", "Phone No.", "Salesperson Code", "Salesperson Number", "EORI Number", "Credit Limit ($)", "Blocked", "Shipment Method Code", "Ship Via Code", "Purchase Order Required", "Payment Terms Code", "Terms Code", "Legacy Addr 1", "Fax No.", "Customer Disc. Group", "Ship-to Code",
    # #                     "Legacy Note ID", "Email", "Country/Region Code", "Country Co          e", "Customer Since", "Legacy Addr 2", "Customer Vertical", "Vertical Code", "Customer Posting Group", "Gen. Bus. Posting Group", "Bill-to Customer No.", "Tax Area Code", "Tax Liable", "Territory Code", "Location Code", "National Account", "SureTax© Exemption Code", "Customer Number"]
    df_columns = ["Customer No.", "Code", "Name", "Address", "Address 2", "City", "State", "ZIP Code", "SureTax© Exemption Code", "Legacy Addr 1",
                  "Shipment Method Code", "Country/Region Code", "Phone No.", "Contact", "Email", "Last Date Modified", "Legacy Addr 2", "Tax Area Code", "Tax Liable", "Customer Number", "Ship-to Number"]

    df = df.reindex(columns=df_columns)
    df = customersNotFound(df)

    df = df.sort_values(by="Last Date Modified").drop_duplicates(
        subset=['Customer No.', 'Address'], keep='last')

    df = assignAddressCode(df)
    print(df)

    df.to_excel("files/to_bc_outputs/ship_address_output.xlsx", index=False)
    df.to_csv("files/to_bc_outputs/ship_address_output.csv", index=False)
    df.to_csv("files/to_bc_outputs/history/ship_address_output_" +
              date_time.strftime("%m%d%y_%H%M%S") + ".csv.gz", index=False, compression='gzip')


def main():
    date_time = datetime.now()
    customer_ids = pd.read_csv("files/relations/customers/ids.csv")
    states = pd.read_csv("files/relations/customers/states.csv")
    suretax = pd.read_csv("files/relations/ship_addresses/suretax.csv")
    country_codes = pd.read_csv(
        "files/relations/customers/country_codes_2.csv")
    shipment_method_code = pd.read_csv(
        "files/relations/customers/shipment_method_code.csv")
    relations_dict = {"customer_ids": customer_ids,
                      "states": states, "country_codes": country_codes, "shipment_method_code": shipment_method_code, "suretax": suretax}

    read_transform(date_time, relations_dict)


if __name__ == "__main__":
    main()
