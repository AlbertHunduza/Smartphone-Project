import re
import numpy as np
import pandas as pd

class GSMArena_Dataset_Cleaner:

    def __init__(self, df):
        self.df = df
        self.cleaning_functions =[self.drop_columns, self.fix_network_technology, self.fix_announced, self.fix_status, self.fix_brand,
                                  self.fix_dimensions, self.fix_weight, self.fix_Build, self.fix_SIM, self.fix_IP_Rating, self.fix_Display_Type,
                                  self.fix_Display_Size, self.fix_Display_Resolution, self.fix_Display_Protection, self.fix_Operating_Software, 
                                  self.fix_Chipset, self.fix_CPU, self.fix_SD_Card_Slot, self.fix_Number_of_Rear_Cameras, self.fix_Camera,
                                  self.fix_Camera_Features, self.fix_Rear_Video, self.fix_Number_of_Selfie_Cameras, self.fix_Selfie_Video,
                                  self.fix_Headphone_Jack, self.fix_WLAN_Technology, self.fix_Bluetooth, self.fix_NFC, self.fix_Radio, self.fix_USB,
                                  self.fix_Sensors, self.fix_UWB, self.fix_Battery, self.fix_Charging, self.fix_Colors, self.fix_Battery_Life,
                                  self.fix_Internal_Storage, self.fix_Storage_Type, self.fix_Loudspeaker, self.fix_price_and_approx_price]





    def drop_columns(self, df):

        """""
        This function drops the columns that are redundant or otherwise not needed for the analysis.
        These columns were created during the scraping process and are not needed for the analysis.
        """""


        to_drop = ['Brands-href', 'Model', 'Models-href', 'Pages', 'Pages-href', 'web-scraper-order', 'web-scraper-start-url', 'GPU', 'Selfie Features'] # Columns to drop

        df.drop(to_drop, inplace=True, axis=1) # Drops the columns
        df = df.rename(columns={'Models': 'Model'}) # Renames the Models column to Model

        return df

    def fix_network_technology(self, df):
    
        """""
        This function fixes the network technology column. It removes the extra information from the column and
        makes it easier to analyze. It takes only the highest network technology and removes the rest.
        """""
            
        df['Network Technology'] = df['Network Technology'].str.split('/').str.get(-1) # Splits the column and takes the last element
        df['Network Technology'] = df['Network Technology'].apply(lambda x: x.strip()) # Removes the extra spaces
        df = df.rename(columns={'Network Technology': 'Highest_Network_Technology'}) # Renames the column
        return df

    def fix_announced(self, df):
    
        """""
        This function fixes the announced column. It splits the information in the column into 2 columns: Announced and Released
        It then converts these to datetime objects for easier analysis, retaining only the year and month.
        """""
            
        # splitting the column into Announced and Released
        df['Released'] = df['Announced'].str.split('.').str.get(-1) # Splits the column and takes the last element - Released
        df['Announced'] = df['Announced'].str.split('.').str.get(0) # Splits the column and takes the first element - Announced

        # working on the released column
        df['Released'] = df['Released'].apply(lambda x: str(x).replace('Released','')) # Removes the word 'Released' from date and converts it to string
        df['Released'] = df['Released'].apply(lambda x: x.strip()) # Removes the extra spaces
        df['Released'] = pd.to_datetime(df['Released'], infer_datetime_format=True, errors='coerce').dt.to_period('M') # Converts the column to datetime and takes only the month and year
        df['Released'] = df['Released'].fillna('Not Released Yet') # Fills the missing values with 'Not Released Yet'

        # working on the announced column
        df['Announced'] = df['Announced'].apply(lambda x: str(x).strip()) # Removes the extra spaces and converts the column to string
        df['Announced'] = pd.to_datetime(df['Announced'], infer_datetime_format=True, errors='coerce').dt.to_period('M') # Converts the column to datetime and takes only the month and year
        df['Announced'] = df['Announced'].fillna('Not Announced Yet') # Fills the missing values with 'Not Announced Yet'
        df.insert(4, "Released", df.pop('Released')) # Moves the released column to the 4th position
        
        return df

    def fix_status(self, df):

        """""
        This function fixes the status column. It splits the status column into 2 columns: Status and Released 2.
        This is necessitated by the fact that some release dates are not available in the Announced column and are instead in the Status column.
        After splitting the column, it converts the Released 2 column to datetime objects for easier analysis, retaining only the year and month.
        Finally it merges the Released column with the Released 2 column and then drops the Released 2 column
        """""
        
        df['Released 2'] = df['Status'].str.split('.').str.get(-1) # Splits the column and takes the last element - Released
        df['Status'] = df['Status'].str.split('.').str.get(0) # Splits the column and takes the first element - Announced


        df['Released 2'] = df['Released 2'].apply(lambda x: str(x).replace('Released','')) # Removes the word 'Released' from date and converts it to string
        df['Released 2'] = df['Released 2'].apply(lambda x: x.strip()) # Removes the extra spaces
        df['Released 2'] = pd.to_datetime(df['Released 2'], infer_datetime_format=True, errors='coerce').dt.to_period('M') # Converts the column to datetime and takes only the month and year
        df['Released'] = np.where(df['Released 2'].notna(), df['Released 2'], df['Released']) # Replaces the released column with the released 2 column if the released 2 column is not null
        df = df.drop(columns=['Released 2']) # Drops the released 2 column
        
        return df


    def fix_brand(self, df):
        """""
        this function fixes the brand column. It removes the extra information from the column,
        leaving only the brand name.

        """""
        df['Brands'] = df['Brands'].str.split("\n").str.get(0)
        df['Brands'] = df['Brands'].str.strip()
        df = df.rename(columns={'Brands': "Brand"})
        return df


    def fix_dimensions(self, df):
        """""
        This function fixes the dimensions column. It splits the column into 3 columns: Height, Width and Thickness.
        It then converts these to float objects for easier analysis.
        Finally it drops the original Dimensions column

        """""

        df['Dimensions'] = df['Dimensions'].str.replace(r'mm\s*\([^)]*\)', '') # removes the dimensions in inches

        df['Length'] = df['Dimensions'].str.split('x').str.get(0) # Splits the column and takes the first element - Length
        df['Width'] = df['Dimensions'].str.split('x').str.get(1) # Splits the column and takes the second element - Width
        df['Thickness'] = df['Dimensions'].str.split('x').str.get(2) # Splits the column and takes the third element - Thickness

        df['Length'] = pd.to_numeric(df['Length'], errors='coerce') # Converts the Length column to float
        df['Width'] = pd.to_numeric(df['Width'], errors='coerce') # Converts the Width column to float
        df['Thickness'] = pd.to_numeric(df['Thickness'], errors='coerce') # Converts the Thickness column to float

        columns_to_move = ["Length", "Width", "Thickness"] # Columns to move
        for i, col in enumerate(columns_to_move, start=6): # Iterates through the columns to move and moves them to the 6th, 7th and 8th positions
            df.insert(i, col, df.pop(col))

        df[['Length', 'Width', 'Thickness']] = df[['Length', 'Width', 'Thickness']].fillna("Not Measured") # Fills the missing values with 'Not Measured'
        df = df.drop(columns=['Dimensions']) # Drops the original Dimensions column

        return df


    def fix_weight(self, df):
        """""
        This function fixes the weight column. removes the weight in oz and converts the column to float objects for easier analysis.
        It then  fills the missing values with 'Not Measured'

        """""
        df['Weight'] = df['Weight'].str.replace(r'g\s*\([^)]*\)', '') # removes the weight in ounces
        df['Weight'] = pd.to_numeric(df['Weight'], errors='coerce') # Converts the Weight column to float
        df['Weight'] = df['Weight'].fillna("Not Measured") # Fills the missing values with 'Not Measured'
        return df


    def fix_Build(self, df):


        """""
        This function fixes the BUILD column. It creates 3 new columns for Front, Back and Frame and extracts that information from
        the BUILD column.
        It then  drops the original BUILD column

        """""


        df['Build'] = df['Build'].apply(lambda x: str(x).split(',')) # Splits the column into a list

        df['Front'] = df['Build'].apply(lambda x: list(filter(lambda x: 'front' in x, x))) # creates the Front column with filtered front information
        df['Back'] = df['Build'].apply(lambda x: list(filter(lambda x: 'back' in x, x))) # creates the Back column with filtered back information
        df['Frame'] = df['Build'].apply(lambda x: list(filter(lambda x: 'frame' in x, x))) # creates the Frame column with filtered frame information

        df['Front'] = df['Front'].astype(str) # converts Front column to string for easier manipulation
        df['Back'] = df['Back'].astype(str) # converts Back column to string for easier manipulation
        df['Frame'] = df['Frame'].astype(str) # converts Frame column to string for easier manipulation


        df['Front'] = df['Front'].str.replace(r'\((.*)\)','').str.replace(r'[\[\]]','').str.replace('front','').str.replace("'",'').str.strip() # removes the extra information from the column
        df['Back'] = df['Back'].str.replace(r'\((.*)\)','').str.replace(r'[\[\]]','').str.replace('back','').str.replace("'",'').str.strip() # removes the extra information from the column
        df['Frame'] = df['Frame'].str.replace(r'\((.*)\)','').str.replace(r'[\[\]]','').str.replace('frame','').str.replace("'",'').str.strip() # removes the extra information from the column

        columns_to_move = ["Front", "Back", "Frame"] # Columns to move
        for i, col in enumerate(columns_to_move, start=10): # Iterates through the columns to move and moves them to the 6th, 7th and 8th positions
            df.insert(i, col, df.pop(col))

        df = df.drop(columns=['Build']) # Drops the original Build column

        return df


    def fix_SIM(self, df):

        """""
        This function fixes the SIM column. It creates two columns for Number of SIMs and Type of SIM.
        It utilizes dictionaries to map the characteristics of the SIM to the categorical values.
        It then  drops the original SIM column

        """""

        df['SIMS'] = df['SIM'].apply(lambda x: str(x).split(' ')) # Splits the column into a list


        number_of_sims_dict = {'2': 2, 'yes': 1, '4': 4, 'dual': 2, 'no' : 0, 'triple' : 3, 'dual': 2, '1' : 1, 'nan': 0} # Dictionary for Number of SIMs
        df['Number_of_SIMs'] = df['SIMS'].apply(lambda x: max([number_of_sims_dict.get(i.lower(), 1) for i in x])) # Creates the Number of SIMs column

        type_of_sim_dict = {'mini-sim': 'Mini-SIM', 'micro-sim': 'Micro-SIM', 'nano-sim': 'Nano-SIM', 'esim': 'eSIM'} # Dictionary for Types of SIM
        df['Type_of_SIM'] = [type_of_sim_dict.get(x[0].lower(), 'Unspecified') for x in df['SIMS']] # Creates the Type of SIM column


        columns_to_move = ["Number_of_SIMs", "Type_of_SIM"] # Columns to move
        for i, col in enumerate(columns_to_move, start=13): # Iterates through the columns to move and moves them to the 13th, 14th positions
            df.insert(i, col, df.pop(col))

        df = df.drop(columns=['SIM', 'SIMS']) # Drops the original Build column and intermediate SIMS column

        return df


    def fix_IP_Rating(self, df):

        """""
        This function fixes the IP Rating column. It eplaces it with a new IP Rating column.
        It looks for key substrings to identify the type of IP Rating a phone has.
        It reports the type of IP Rating a phone has.

        """""

        df["IP_Rating"] = df["IP Rating"].apply(lambda x: 
            next((x for x in x.split() if x.startswith("IP")), "water/dust resistant") if not pd.isnull(x) and any(y in x for y in ["proof", "resistant", "repellant", "protection"]) else 
            "Military-Grade Certification" if not pd.isnull(x) and any(y in x for y in ["MIL"]) else
            "Weather-Sealed Ports" if not pd.isnull(x) and "sealed" in x else 
            "Self-Healing Capability" if not pd.isnull(x) and "healing" in x else
            "No Resistance"
        ) # Creates the IP Rating column by looking for key substrings and categorizing them

        df = df.drop(columns=['IP Rating']) # Drops the original IP Rating column

        
        return df

    def fix_Display_Type(self, df):
        """""
        This function fixes the Display Type column. It creates two new columns for Display Brightness and HDR Capability.
        It combs through the information in the original Display Type column and extracts the relevant information for 
        Type, Brightness, and HDR Capability

        """""

        # creating HDR Capability Column
        df["Display_HDR_Capability"] = df["Display Type"].apply(lambda x: 
            "HDR10+" if not pd.isnull(x) and x.lower().find("hdr10+") != -1 else
            "HDR10" if not pd.isnull(x) and x.lower().find("hdr10") != -1 else
            "HDR" if not pd.isnull(x) and x.lower().find("hdr") != -1 else
            "No HDR"
        ) # creates the HDR capability column by finding the key substrings "hdr", "hdr10", and "hdr10+"


        # creating Display Brightness column
        df["Display_Brightness"] = df["Display Type"].apply(lambda x:
            next((substring for i, substring in enumerate(str(x).split()) if substring.isnumeric() and x.split()[i+1] == "nits"), "Not Specified") if x is not None else "Not specified"
        ) # creates the Display Brightness column by looking for the number of nits in the Display Type column

        df["Display_Brightness"] = df["Display_Brightness"].replace("Not Specified", np.nan) # replaces the "Not Specified" with NaNs
        df["Display_Brightness"] = df["Display_Brightness"].astype(float) # converts the column to float
        df["Display_Brightness"] = df["Display_Brightness"].fillna("Not Specified") # replaces the NaNs with "Not Specified"

            
        # updating the Display Type column
        df["Display_Type"] = df["Display Type"].apply(lambda x: 
            "CSTN" if not pd.isnull(x) and x.lower().find("cstn") != -1 else
            "TFT" if not pd.isnull(x) and x.lower().find("tft") != -1 else
            "Foldable LTPO AMOLED" if not pd.isnull(x) and x.lower().find("foldable ltpo amoled") != -1 else
            "Foldable AMOLED" if not pd.isnull(x) and x.lower().find("foldable amoled") != -1 else
            "LTPO AMOLED" if not pd.isnull(x) and x.lower().find("ltpo amoled") != -1 else
            "LTPO2 AMOLED" if not pd.isnull(x) and x.lower().find("ltpo2 amoled") != -1 else
            "LTPO3 AMOLED" if not pd.isnull(x) and x.lower().find("ltpo3 amoled") != -1 else
            "LTPO4 AMOLED" if not pd.isnull(x) and x.lower().find("ltpo4 amoled") != -1 else
            "Super AMOLED" if not pd.isnull(x) and x.lower().find("super amoled") != -1 else
            "Dynamic AMOLED" if not pd.isnull(x) and x.lower().find("dynamic amoled") != -1 else
            "AMOLED" if not pd.isnull(x) and x.lower().find("amoled") != -1 else
            "Super AMOLED" if not pd.isnull(x) and x.lower().find("super amoled") != -1 else
            "OLED" if not pd.isnull(x) and x.lower().find("oled") != -1 else 
            "STN" if not pd.isnull(x) and x.lower().find("stn") != -1 else
            "TFD" if not pd.isnull(x) and x.lower().find("tfd") != -1 else
            "LCD" if not pd.isnull(x) and x.lower().find("lcd") != -1 else
            "IPS LCD" if not pd.isnull(x) and x.lower().find("ips") != -1 else
            "IGZO" if not pd.isnull(x) and x.lower().find("igzo") != -1 else
            "CGS" if not pd.isnull(x) and x.lower().find("CGS") != -1 else
            "Monochrome" if not pd.isnull(x) and x.lower().find("monochrome") != -1 else
            "TN" if not pd.isnull(x) and x.lower().find("tn") != -1 else
            "Not Specified"
        ) # creates the Display Type column by looking for the key substrings "cstn", "tft", "ltpo", "amoled", "oled", "stn", "tfd", "lcd", "ips", "igzo", "cgs", "monochrome", and "tn"

        columns_to_move = ["Display_Type", "Display_Brightness", "Display_HDR_Capability"] # Columns to move
        for i, col in enumerate(columns_to_move, start=16): # Iterates through the columns to move and moves them to the 16th, 18th positions
            df.insert(i, col, df.pop(col))

        df = df.drop(columns=['Display Type']) # Drops the original Display Type column


        return df

    def fix_Display_Size(self, df):
        """""
        This function fixes the Display Size Column. It extracts the most important information from this column
        and creates 2 new columns with it - Screen to body ratio and Display size in inches
        Finally, it drops the original Display Size Column

        """""

        df["Screen_To_Body_Ratio_(%)"] = df["Display Size"].apply(lambda x: float(re.findall(r'~(.*?)%', x)[0]) 
        if not pd.isnull(x) and str(x).strip() != "" and re.findall(r'~(.*?)%', x) else "Not Measured") # creates screen to body ratio column by finding the number between the ~ and % signs
        df["Display_Size_(inches)"] = df["Display Size"].apply(lambda x: float(x[:x.find(" inches")].strip().split()[-1])
        if x is not None and str(x).strip() != "" and str(x).find(" inches") != -1 else "Not Measured") # creates the Display size column by finding the number before the "inches" substring

        columns_to_move = ["Display_Size_(inches)", "Screen_To_Body_Ratio_(%)"] # Columns to move
        for i, col in enumerate(columns_to_move, start=19): # Iterates through the columns to move and moves them to the 19th, 20th positions
            df.insert(i, col, df.pop(col))

        df = df.drop(columns='Display Size') # Drops the original Display Size column

        return df

    def fix_Display_Resolution(self, df):

        """""
        This function fixes the Display Resolution column. It extracts the most important information and creates 2 new columns with 
        it - Aspect Ratio and Pixel Density. Finally, it drops the original Display Resolution column

        """"" 

        df["Display_Aspect_Ratio"] = df["Display Resolution"].apply(lambda x: x[:x.find(" ratio")].strip().split()[-1]
        if x is not None and str(x).strip() != "" and str(x).find(" ratio") != -1 else "Not Measured") # creates the Aspect Ratio column by finding the number before the "ratio" substring

        df["Pixel_Density"] = df["Display Resolution"].apply(lambda x: x[:x.find(" ppi")].strip().split()[-1]
        if x is not None and str(x).strip() != "" and str(x).find(" ppi") != -1 else "Not Measured") # creates the Pixel Density column by finding the number before the "ppi" substring
        df["Pixel_Density"] = df["Pixel_Density"].apply(lambda x: int(x.replace("(~",'')) if x != "Not Measured" and "~" in x else x) # removes the (~ from the Pixel Density column

        columns_to_move = ["Display_Aspect_Ratio", "Pixel_Density"] # Columns to move
        for i, col in enumerate(columns_to_move, start=21): # Iterates through the columns to move and moves them to the 21st, 22nd positions
            df.insert(i, col, df.pop(col))

        df = df.drop(columns='Display Resolution') # Drops the original Display Size column

        return df

    def fix_Display_Protection(self, df):
        """""
        This function fixes the Display Protection column. It extracts the most display protective glass information and creates a new column with 
        it - Display_Protection. Finally, it drops the original Display Protection column

        """""


        df["Display_Protection"] = df["Display Protection"].apply(lambda x: 
            # gorilla glasses
            "Corning Gorilla Glass 1" if not pd.isnull(x) and x.lower().find("gorilla glass 1") != -1 else
            "Corning Gorilla Glass 2" if not pd.isnull(x) and x.lower().find("gorilla glass 2") != -1 else
            "Corning Gorilla Glass 3" if not pd.isnull(x) and x.lower().find("gorilla glass 3") != -1 else
            "Corning Gorilla Glass 3+" if not pd.isnull(x) and x.lower().find("gorilla glass 3+") != -1 else
            "Corning Gorilla Glass 4" if not pd.isnull(x) and x.lower().find("gorilla glass 4") != -1 else
            "Corning Gorilla Glass 5" if not pd.isnull(x) and x.lower().find("gorilla glass 5") != -1 else
            "Corning Gorilla Glass 6" if not pd.isnull(x) and x.lower().find("gorilla glass 6") != -1 else
            "Corning Gorilla Glass Victus+" if not pd.isnull(x) and x.lower().find("gorilla glass victus+") != -1 else
            "Corning Gorilla Glass Victus" if not pd.isnull(x) and x.lower().find("gorilla glass victus") != -1 else
            "Corning Gorilla Glass DX" if not pd.isnull(x) and x.lower().find("gorilla glass dx") != -1 else
            "Corning Gorilla Glass DX+" if not pd.isnull(x) and x.lower().find("gorilla glass dx+") != -1 else
            "Corning Gorilla Glass (unspecified)" if not pd.isnull(x) and x.lower().find("gorilla glass") != -1 else
            
            # dragontrail
            "Asahi Dragontail Glass Pro" if not pd.isnull(x) and x.lower().find("dragontail glass pro") != -1 else
            "Asahi Dragontail Glass" if not pd.isnull(x) and x.lower().find("dragontail") != -1 else

            # sapphire
            "Sapphire Crystal Glass" if not pd.isnull(x) and x.lower().find("sapphire") != -1 else

            # Huawei
            "Huawei Kunlun Glass" if not pd.isnull(x) and x.lower().find("huawei") != -1 else

            "Rainbow Glass" if not pd.isnull(x) and x.lower().find("rainbow") != -1 else
            "Innolux Glass" if not pd.isnull(x) and x.lower().find("innolux") != -1 else

            "Ceramic Shield Glass" if not pd.isnull(x) and x.lower().find("ceramic") != -1 else
            "Yes (unspecified)" if not pd.isnull(x) and x.lower().find("yes") != -1 else
            "AGC Glass" if not pd.isnull(x) and x.lower().find("agc") != -1 else
            "Panda Glass" if not pd.isnull(x) and x.lower().find("panda") != -1 else

            "Schott UTG Glass" if not pd.isnull(x) and x.lower().find("schott utg") != -1 else
            "Schott Xensation Glass" if not pd.isnull(x) and x.lower().find("schott") != -1 else
            "NEG Dinorex T2X-1 Glass" if not pd.isnull(x) and x.lower().find("dino") != -1 else
            "NEG Dinorex T2X-1 Glass" if not pd.isnull(x) and x.lower().find("t2x") != -1 else
            "NEG Dinorex T2X-1 Glass" if not pd.isnull(x) and x.lower().find("neg") != -1 else
            "Ion-strengthened Glass" if not pd.isnull(x) and x.lower().find("ion") != -1 else
            "Ion-strengthened Glass" if not pd.isnull(x) and x.lower().find("strength") != -1 else



            "no special protection"
        )
        columns_to_move = ["Display_Protection"] # Columns to move
        for i, col in enumerate(columns_to_move, start=23): # Iterates through the columns to move and moves them to the 23rd position
            df.insert(i, col, df.pop(col))

        df = df.drop(columns='Display Protection') # Drops the original Display Protection column

        return df

    def fix_Operating_Software(self, df):
        """""
        This function fixes the Operating Software column. It extracts the most common operating system information and creates a new column with
        it - Mobile_OS. It retains the original information in the Mobile_OS_Version column.
        Finally, it drops the original Operating Software column

        """""


        df["Mobile_OS"] = df["Operating Software"].apply(lambda x: 
            # Android
            "Android" if not pd.isnull(x) and x.lower().find("android") != -1 else # If the value is not null and the string contains "android", then it is Android
                
                
            # Apple
            "iOS" if not pd.isnull(x) and x.lower().find("ios") != -1 else # If the value is not null and the string contains "ios", then it is iOS
            "iPad OS" if not pd.isnull(x) and x.lower().find("ipad") != -1 else # If the value is not null and the string contains "ipad", then it is iPad OS
            "watchOS" if not pd.isnull(x) and x.lower().find("watchos") != -1 else # If the value is not null and the string contains "watchos", then it is watchOS

            # Blackerry
            "Blackberry Tablet OS" if not pd.isnull(x) and x.lower().find("blackberry tablet") != -1 else # If the value is not null and the string contains "blackberry tablet", then it is Blackberry Tablet OS
            "Blackberry OS" if not pd.isnull(x) and x.lower().find("blackberry") != -1 else # If the value is not null and the string contains "blackberry", then it is Blackberry OS

            # Huawei
            "HarmonyOS" if not pd.isnull(x) and x.lower().find("harmony") != -1 else # If the value is not null and the string contains "harmony", then it is HarmonyOS
            "Huawei Lite OS" if not pd.isnull(x) and x.lower().find("huawei lite") != -1 else # If the value is not null and the string contains "huawei lite", then it is Huawei Lite OS
            "EMUI" if not pd.isnull(x) and x.lower().find("emui") != -1 else # If the value is not null and the string contains "emui", then it is EMUI

            # Microsoft
            "Microsoft Windows Mobile" if not pd.isnull(x) and x.lower().find("microsoft windows mobile") != -1 else # If the value is not null and the string contains "microsoft windows mobile", then it is Microsoft Windows Mobile
            "Microsoft Windows Phone" if not pd.isnull(x) and x.lower().find("microsoft windows phone") != -1 else # If the value is not null and the string contains "microsoft windows phone", then it is Microsoft Windows Phone
            "Microsoft Windows PocketPC" if not pd.isnull(x) and x.lower().find("microsoft windows pocketpc") != -1 else # If the value is not null and the string contains "microsoft windows pocketpc", then it is Microsoft Windows PocketPC
            "Microsoft Smartphone" if not pd.isnull(x) and x.lower().find("microsoft smartphone") != -1 else # If the value is not null and the string contains "microsoft smartphone", then it is Microsoft Smartphone
            "Microsoft Windows" if not pd.isnull(x) and x.lower().find("microsoft windows") != -1 else # If the value is not null and the string contains "microsoft windows", then it is Microsoft Windows
            "Microsoft Windows" if not pd.isnull(x) and x.lower().find("wince") != -1 else # If the value is not null and the string contains "wince", then it is Microsoft Windows

            # Nokia
            "Symbian" if not pd.isnull(x) and x.lower().find("symbian") != -1 else # If the value is not null and the string contains "symbian", then it is Symbian
            "Nokia Asha Software Platform" if not pd.isnull(x) and x.lower().find("nokia asha") != -1 else # If the value is not null and the string contains "nokia asha", then it is Nokia Asha Software Platform
            "Internet Tablet OS" if not pd.isnull(x) and x.lower().find("internet tablet") != -1 else # If the value is not null and the string contains "internet tablet", then it is Internet Tablet OS
            
            # Samsung
            "Bada OS" if not pd.isnull(x) and x.lower().find("bada") != -1 else # If the value is not null and the string contains "bada", then it is Bada OS
            "Tizen OS" if not pd.isnull(x) and x.lower().find("tizen") != -1 else # If the value is not null and the string contains "tizen", then it is Tizen OS
            "Touchwiz Lite UI" if not pd.isnull(x) and x.lower().find("touchwiz lite ui") != -1 else # If the value is not null and the string contains "touchwiz lite ui", then it is Touchwiz Lite UI

            # Others
            "Firefox OS" if not pd.isnull(x) and x.lower().find("firefox") != -1 else  # If the value is not null and the string contains "firefox", then it is Firefox OS
            "Proprietary OS" if not pd.isnull(x) and x.lower().find("proprietary") != -1 else # If the value is not null and the string contains "proprietary", then it is Proprietary OS
            "Linux" if not pd.isnull(x) and x.lower().find("linux") != -1 else # If the value is not null and the string contains "linux", then it is Linux
            "LiMo OS" if not pd.isnull(x) and x.lower().find("limo") != -1 else # If the value is not null and the string contains "limo", then it is LiMo OS
            "Danger" if not pd.isnull(x) and x.lower().find("danger") != -1 else # If the value is not null and the string contains "danger", then it is Danger
            "Sonim OS" if not pd.isnull(x) and x.lower().find("sonim") != -1 else  # If the value is not null and the string contains "sonim", then it is Sonim OS
            "Palm OS" if not pd.isnull(x) and x.lower().find("palm") != -1 else # If the value is not null and the string contains "palm", then it is Palm OS
            "KaiOS" if not pd.isnull(x) and x.lower().find("kai") != -1 else # If the value is not null and the string contains "kai", then it is KaiOS
            "Maemo OS" if not pd.isnull(x) and x.lower().find("maemo") != -1 else # If the value is not null and the string contains "maemo", then it is Maemo OS
            "MeeGo" if not pd.isnull(x) and x.lower().find("meego") != -1 else # If the value is not null and the string contains "meego", then it is MeeGo
            "Sailfish OS" if not pd.isnull(x) and x.lower().find("sailfish") != -1 else # If the value is not null and the string contains "sailfish", then it is Sailfish OS
            "Ubuntu Touch" if not pd.isnull(x) and x.lower().find("ubuntu touch") != -1 else # If the value is not null and the string contains "ubuntu touch", then it is Ubuntu Touch
            "Ubuntu" if not pd.isnull(x) and x.lower().find("ubuntu") != -1 else # If the value is not null and the string contains "ubuntu", then it is Ubuntu
            "WebOS" if not pd.isnull(x) and x.lower().find("webos") != -1 else # If the value is not null and the string contains "webos", then it is WebOS
            "Flyme OS" if not pd.isnull(x) and x.lower().find("flyme") != -1 else # If the value is not null and the string contains "flyme", then it is Flyme OS
            "Moto Watch OS" if not pd.isnull(x) and x.lower().find("moto watch") != -1 else # If the value is not null and the string contains "moto watch", then it is Moto Watch OS
            "Chrome OS" if not pd.isnull(x) and x.lower().find("chrome") != -1 else # If the value is not null and the string contains "chrome", then it is Chrome OS

            "unspecified" # If none of the above conditions are met, then it is unspecified
        )

        df["Mobile_OS_Version"] = df["Operating Software"].str.strip()
        df['Mobile_OS_Version'] = df['Mobile_OS_Version'].fillna('unspecified') # Fills the missing values with 'unspecified'

        columns_to_move = ["Mobile_OS", "Mobile_OS_Version"] # Columns to move
        for i, col in enumerate(columns_to_move, start=24): # Iterates through the columns to move and moves them to the 24th and 25th positions
            df.insert(i, col, df.pop(col))

        df = df.drop(columns='Operating Software') # Drops the original Display Protection column

        return df


    def fix_Chipset(self, df):
        """""
        This function fixes the Chipset column. It extracts the Chipset maker information and creates a new column with
        it - Chipset_Maker. It then extracts the fabrication process information and forms a new column of integers
        with it - 'Fabrication_Process'.
        Finally, it drops the original Chipset column

        """""


        df["Chipset_Maker"] = df["Chipset"].apply(lambda x: 
            # checks
            "Qualcomm" if not pd.isnull(x) and x.lower().find("qualcomm") != -1 else # If the value is not null and the string contains "qualcomm", then it is Qualcomm
            "MediaTek" if not pd.isnull(x) and x.lower().find("mediatek") != -1 else # If the value is not null and the string contains "mediatek", then it is MediaTek
            "MediaTek" if not pd.isnull(x) and x.lower().find("mt") != -1 else # If the value is not null and the string contains "mt", then it is MediaTek
            "NVIDIA" if not pd.isnull(x) and x.lower().find("nvidia") != -1 else # If the value is not null and the string contains "nvidia", then it is NVIDIA
            "Marvell" if not pd.isnull(x) and x.lower().find("marvell") != -1 else # If the value is not null and the string contains "marvell", then it is Marvell
            "Intel" if not pd.isnull(x) and x.lower().find("intel") != -1 else # If the value is not null and the string contains "intel", then it is Intel
            "Qualcomm" if not pd.isnull(x) and x.lower().find("msm") != -1 else # If the value is not null and the string contains "msm", then it is Qualcomm
            "UNISOC" if not pd.isnull(x) and x.lower().find("spreadtrum") != -1 else # If the value is not null and the string contains "spreadtrum", then it is UNISOC
            "UNISOC" if not pd.isnull(x) and x.lower().find("unisoc") != -1 else # If the value is not null and the string contains "unisoc", then it is UNISOC
            "UNISOC" if not pd.isnull(x) and x.lower().find("sc") != -1 else # If the value is not null and the string contains "sc", then it is UNISOC
            "Allwinner" if not pd.isnull(x) and x.lower().find("allwinner") != -1 else # If the value is not null and the string contains "allwinner", then it is Allwinner
            "Broadcom" if not pd.isnull(x) and x.lower().find("bcm") != -1 else # If the value is not null and the string contains "bcm", then it is Broadcom
            "Broadcom" if not pd.isnull(x) and x.lower().find("broadcom") != -1 else # If the value is not null and the string contains "broadcom", then it is Broadcom
            "Xiaomi" if not pd.isnull(x) and x.lower().find("xiaomi") != -1 else # If the value is not null and the string contains "xiaomi", then it is Xiaomi
            "Leadcore" if not pd.isnull(x) and x.lower().find("leadcore") != -1 else # If the value is not null and the string contains "leadcore", then it is Leadcore
            "JLQ" if not pd.isnull(x) and x.lower().find("jlq") != -1 else # If the value is not null and the string contains "jlq", then it is JLQ
            "Rockchip" if not pd.isnull(x) and x.lower().find("rockchip") != -1 else # If the value is not null and the string contains "rockchip", then it is Rockchip
            "Rockchip" if not pd.isnull(x) and x.lower().find("rochip") != -1 else # If the value is not null and the string contains "rochip", then it is Rockchip
            "Exynos" if not pd.isnull(x) and x.lower().find("exynos") != -1 else # If the value is not null and the string contains "exynos", then it is Exynos
            "TI" if not pd.isnull(x) and x.lower().find("ti") != -1 else # If the value is not null and the string contains "ti", then it is TI
            "Hummingbird" if not pd.isnull(x) and x.lower().find("hummingbird") != -1 else # If the value is not null and the string contains "hummingbird", then it is Hummingbird
            "Apple" if not pd.isnull(x) and x.lower().find("apple") != -1 else # If the value is not null and the string contains "apple", then it is Apple
            "NovaThor" if not pd.isnull(x) and x.lower().find("novathor") != -1 else # If the value is not null and the string contains "novathor", then it is NovaThor
            "Rockchip" if not pd.isnull(x) and x.lower().find("rockchip") != -1 else # If the value is not null and the string contains "rockchip", then it is Rockchip
            "Philips" if not pd.isnull(x) and x.lower().find("philips") != -1 else # If the value is not null and the string contains "philips", then it is Philips
            "Pega-Dual" if not pd.isnull(x) and x.lower().find("pega") != -1 else # If the value is not null and the string contains "pega", then it is Pega-Dual
            "LG" if not pd.isnull(x) and x.lower().find("lg") != -1 else # If the value is not null and the string contains "lg", then it is LG
            "Intel®" if not pd.isnull(x) and x.lower().find("intel") != -1 else # If the value is not null and the string contains "intel", then it is Intel
            "Vivante" if not pd.isnull(x) and x.lower().find("vivante") != -1 else # If the value is not null and the string contains "vivante", then it is Vivante
            "Huawei" if not pd.isnull(x) and x.lower().find("huawei") != -1 else # If the value is not null and the string contains "huawei", then it is Huawei
            "Kirin" if not pd.isnull(x) and x.lower().find("kirin") != -1 else # If the value is not null and the string contains "kirin", then it is Kirin
            "ATI" if not pd.isnull(x) and x.lower().find("ati") != -1 else # If the value is not null and the string contains "ati", then it is ATI
            "HiSilicon" if not pd.isnull(x) and x.lower().find("hisilicon") != -1 else # If the value is not null and the string contains "hisilicon", then it is HiSilicon
            "Google" if not pd.isnull(x) and x.lower().find("google") != -1 else # If the value is not null and the string contains "google", then it is Google
            "Infineon" if not pd.isnull(x) and x.lower().find("infineon") != -1 else # If the value is not null and the string contains "infineon", then it is Infineon
            "Nordic Semiconductor" if not pd.isnull(x) and x.lower().find("nrf") != -1 else # If the value is not null and the string contains "nrf", then it is Nordic Semiconductor
            "RDA" if not pd.isnull(x) and x.lower().find("rda") != -1 else # If the value is not null and the string contains "rda", then it is RDA
            "VIA" if not pd.isnull(x) and x.lower().find("via") != -1 else # If the value is not null and the string contains "via", then it is VIA
            "STM" if not pd.isnull(x) and x.lower().find("st") != -1 else # If the value is not null and the string contains "st", then it is STM
            "Qualcomm" if not pd.isnull(x) and x.lower().find("snapdragon") != -1 else # If the value is not null and the string contains "snapdragon", then it is Qualcomm
            "Qualcomm" if not pd.isnull(x) and x.lower().find("qm") != -1 else # If the value is not null and the string contains "qm", then it is Qualcomm
        
            "unspecified" # If none of the above conditions are met, then it is unspecified
        )

        df['Fabrication_Process'] = df['Chipset'].str.extract("\(([^)]+)\)", expand=False).str.replace("nm", "") # Extracts the fabrication process from the Chipset column and removes the "nm" from the end of the string
        df['Fabrication_Process'] = df['Fabrication_Process'].where(df['Chipset'].str.contains("\("), "unspecified") # If the Chipset column does not contain "(", then the fabrication process is unspecified
        df['Fabrication_Process'] = pd.to_numeric(df['Fabrication_Process'], errors='coerce').fillna(0).astype(int) # Converts the fabrication process to an integer

        columns_to_move = ["Chipset_Maker", "Fabrication_Process"] # Columns to move
        for i, col in enumerate(columns_to_move, start=26): # Iterates through the columns to move and moves them to the 26th and 27th positions
            df.insert(i, col, df.pop(col))

        df = df.drop(columns=["Chipset"]) # Drops the original Chipset column
        
        return df

    def fix_CPU(self, df):

        """""
        This function fixes the CPU column by extracting the CPU speed and the number of cores from the CPU column.
        It creates three new columns: Number_of_CPU_Cores CPU_Perfomance_Core_Frequency and CPU_Perfomance_Core_Frequency.
        It also drops the original CPU column.

        """""


        df["Number_of_CPU_Cores"] = df["CPU"].apply(lambda x: 
            
            "Unspecified" if pd.isnull(x) else # checks for NaN values

            int(2) if x.lower().find("dual") != -1 else # If the value is not null and the string contains "dual", then it is dual core
            int(4) if x.lower().find("quad") != -1 else # If the value is not null and the string contains "quad", then it is quad core
            int(6) if x.lower().find("hexa") != -1 else # If the value is not null and the string contains "hexa", then it is hexa core
            int(8) if x.lower().find("octa") != -1 else # If the value is not null and the string contains "octa", then it is octa core
            int(10) if x.lower().find("deca") != -1 else # If the value is not null and the string contains "deca", then it is deca core
            int(12) if x.lower().find("dodeca") != -1 else # If the value is not null and the string contains "dodeca", then it is dodeca core
            int(16) if x.lower().find("hexadeca") != -1 else # If the value is not null and the string contains "hexadeca", then it is hexadeca core

            int(1) # If none of the above conditions are met, then assume 1 core

        )

        def extract_max_processor_frequency(value): # Extracts the maximum processor frequency from the CPU column
            if value is not np.nan: # Checks for NaN values
                freq = re.findall(r'\d+\.\d+\s*(?:GHz)|\d+\d+\s*(?:MHz)', value) # Finds all the frequencies in the string
                
                if freq: # Checks if the list is not empty
                    freq = [f.split(" ")[0] for f in freq] # Splits the string at the space and takes the first element
                    freq = [float(f.replace("GHz", "").replace("MHz", "")) for f in freq] # Removes the "GHz" and "MHz" from the string and converts it to a float
                    max_freq = max(freq) # Finds the maximum frequency
                    if max_freq >= 100: # Checks if the frequency is in MHz
                        return max_freq/1000 # Converts the frequency to GHz
                    else:
                        return max_freq # Returns the frequency in GHz
                else:
                    return np.nan # Returns NaN if the list is empty
            else:
                return np.nan # Returns NaN if the value is NaN

        def extract_min_processor_frequency(value): # Extracts the minimum processor frequency from the CPU column
            if value is not np.nan: # Checks for NaN values
                freq = re.findall(r'\d+\.\d+\s*(?:GHz)|\d+\d+\s*(?:MHz)', value) # Finds all the frequencies in the string
                if freq: # Checks if the list is not empty
                    freq = [f.split(" ")[0] for f in freq] # Splits the string at the space and takes the first element
                    freq = [float(f.replace("GHz", "").replace("MHz", "")) for f in freq] # Removes the "GHz" and "MHz" from the string and converts it to a float
                    min_freq =  min(freq) # Finds the minimum frequency
                    if min_freq >= 100: # Checks if the frequency is in MHz
                        return min_freq/1000 # Converts the frequency to GHz
                    else:
                        return min_freq # Returns the frequency in GHz
                else:
                    return np.nan # Returns NaN if the list is empty
            else:
                return np.nan # Returns NaN if the value is NaN

        def extract_processor_frequency_df(df): # Extracts the maximum and minimum processor frequencies from the CPU column
            df['CPU_Performance_Core_Frequency'] = df['CPU'].apply(extract_max_processor_frequency) # Extracts the maximum processor frequency
            df['CPU_Efficiency_Core_Frequency'] = df['CPU'].apply(extract_min_processor_frequency) # Extracts the minimum processor frequency
            df['CPU_Performance_Core_Frequency'].fillna(value="unspecified", inplace=True) # Fills the NaN values with "unspecified"
            df['CPU_Efficiency_Core_Frequency'].fillna(value="unspecified", inplace=True) # Fills the NaN values with "unspecified"
            return df

        df = extract_processor_frequency_df(df) # Extracts the maximum and minimum processor frequencies from the CPU column

        columns_to_move = ['Number_of_CPU_Cores', 'CPU_Performance_Core_Frequency', 'CPU_Efficiency_Core_Frequency'] # Columns to move
        for i, col in enumerate(columns_to_move, start=28): # Iterates through the columns to move and moves them to the 28th and 29th and 30thpositions
            df.insert(i, col, df.pop(col)) # Moves the columns

        df = df.drop(columns=["CPU"]) # Drops the original CPU column

        return df

    def fix_SD_Card_Slot(self, df):
        """""
        This function fixes the SD Card Slot column. It specifies the type of SD Card Slot. Unless "no" is pecified, it is assumed that the phone has an SD Card Slot.

        """""


        df["SD Card Slot"] = df["SD Card Slot"].apply(lambda x: 
            # checks
            "No" if not pd.isnull(x) and x.lower().find("no") != -1 else # If the value is not null and the string contains "no", then it is no
            "microSDHC" if not pd.isnull(x) and x.lower().find("microsdhc") != -1 else # If the value is not null and the string contains "microsdhc", then it is microSDHC
            "microSDXC" if not pd.isnull(x) and x.lower().find("microsdxc") != -1 else # If the value is not null and the string contains "microsdxc", then it is microSDXC
            "microSD" if not pd.isnull(x) and x.lower().find("microsd") != -1 else # If the value is not null and the string contains "microsd", then it is microSD
            "miniSD" if not pd.isnull(x) and x.lower().find("minisd") != -1 else # If the value is not null and the string contains "minisd", then it is miniSD
            "SDHC" if not pd.isnull(x) and x.lower().find("sdhc") != -1 else # If the value is not null and the string contains "sdhc", then it is SDHC
            "SDXC" if not pd.isnull(x) and x.lower().find("sdxc") != -1 else # If the value is not null and the string contains "sdxc", then it is SDXC
            "SD" if not pd.isnull(x) and x.lower().find("sd") != -1 else # If the value is not null and the string contains "sd", then it is SD
            "Memory Stick Duo Pro" if not pd.isnull(x) and x.lower().find("memory stick duo pro") != -1 else # If the value is not null and the string contains "memory stick duo pro", then it is Memory Stick Duo Pro
            "Memory Stick Duo" if not pd.isnull(x) and x.lower().find("memory stick duo") != -1 else # If the value is not null and the string contains "memory stick duo", then it is Memory Stick Duo
            "Memory Stick" if not pd.isnull(x) and x.lower().find("memory stick") != -1 else # If the value is not null and the string contains "memory stick pro duo", then it is Memory Stick Pro Duo
            "Yes" # If none of the above conditions are met, then assume yes
        )

        return df


        
    def fix_Number_of_Rear_Cameras(self, df):
        """""
        This function fixes the Number of Rear Cameras column. It specifies the number of rear cameras. Unless "no" is pecified,
        It is assumed that the phone has a rear camera. The number of rear cameras is implied from the string eg. "Penta" implies 5 rear cameras.
        If both the Numner of Rear Cameras and the Camera column are null, then the number of rear cameras is assumed to be 0.
        if the Number of Rear Cameras column is null, but the Camera column is not null, then the number of rear cameras is assumed to be 1.

        """""

        def has_camera_or_not(x):
            """""
            This function checks if the phone has a camera or not. 
            If the string in the Camera column contains "no" or is null, 
            then it is assumed that the phone does not have a camera.

            """""

            if pd.isnull(x) or x.lower().find("no") != -1: # If the value is null or the string contains "no", then it has no camera
                return 0 # Returns 0
            return np.nan # Returns NaN for all rows with a camera
        
        def get_num_cameras(x):

            """""
            This function extracts the number of cameras from the string in the Number of Rear Cameras column.
            Its results are used to fill the NaN values with the values with the extracted number of cameras.

            """""

            if not isinstance(x, str): # If the value is not a string, then return 1
                return 1 # Returns deafault value 1 for all rows with a camera
            
            if x.lower().find("penta") != -1: # If the string contains "penta", then return 5
                return 5
            if x.lower().find("five") != -1: # If the string contains "five", then return 5
                return 5
            if x.lower().find("quad") != -1: # If the string contains "quad", then return 4
                return 4
            if x.lower().find("triple") != -1: # If the string contains "triple", then return 3
                return 3
            if x.lower().find("dual") != -1: # If the string contains "dual", then
                return 2
            if x.lower().find("single") != -1: # If the string contains "single", then return 1
                return 1    

        df["Number_of_Rear_Cameras"] = df["Camera"].apply(lambda x: has_camera_or_not(x)) # Creates a new column with the results of the has_camera_or_not function
        
        df["Number_of_Rear_Cameras"] = df["Number_of_Rear_Cameras"].fillna(df["Number of Rear Cameras"].apply(lambda x: get_num_cameras(x))) # Fills the NaN values with the results of the get_num_cameras function

        columns_to_move = ['Number_of_Rear_Cameras'] # Columns to move
        for i, col in enumerate(columns_to_move, start=33): # Iterates through the columns to move and moves them to the 33rd
            df.insert(i, col, df.pop(col)) # Moves the columns

        df = df.drop(columns=["Number of Rear Cameras"]) # Drops the original Number of Rear Cameras column
        
        return df

    def fix_Camera(self, df):

        """""
        This function fixes the Camera column. It extracts the highest camera resolution from the string in the Camera column.
        If the string contains "VGA", then the highest camera resolution is assumed to be VGA.
        If the string contains "no", "no camera" is returned.
        The highest camera resolution is assumed to be the maximum value in the string that appears before "MP".
        It is returned as an integer and the original Camera column is dropped.

        """""

        def extract_highest_camera_resolution(val): # Extracts the highest camera resolution from the string in the Camera column
            if pd.isna(val): # If the value is null, then return "no camera"
                return "no camera"
            elif "VGA" in val: # If the string contains "VGA", then return "VGA"
                return "VGA"
            else:
                pattern = r"\b(\d+\.\d+|\d+)\b MP" # Regex pattern to extract the highest camera resolution
                resolutions = re.findall(pattern, val) # Finds all the matches in the string
                try:
                    return int(max(resolutions, key=float)) # Returns the maximum value in the string that appears before "MP"
                except ValueError: # If the string does not contain any numbers, then
                    return "no camera" # Returns "no camera" if the string does not contain any numbers

        df["Highest_Camera_Resolution"] = df["Camera"].apply(lambda x: extract_highest_camera_resolution(x)) # Creates a new column with the results of the extract_highest_camera_resolution function


        columns_to_move = ['Highest_Camera_Resolution'] # Columns to move
        for i, col in enumerate(columns_to_move, start=34): # Iterates through the columns to move and moves them to the 34th position
            df.insert(i, col, df.pop(col)) # Moves the columns

        df = df.drop(columns=["Camera"]) # Drops the original Camera column

        return df

    def fix_Camera_Features(self, df):
        """""
        This function fixes the Camera Features column. Essentially, it looks at whether a camera has HDR or not.
        If the string contains "HDR", then it is assumed that the camera has HDR.

        """""


        df["HDR"] = df["Camera Features"].apply(lambda x: 
            # checks
            "HDR" if not pd.isnull(x) and x.lower().find("hdr") != -1 else # If the string contains "HDR", then return "HDR"
            "no HDR" # Returns "no HDR" for all other rows

        )

        columns_to_move = ['HDR'] # Columns to move
        for i, col in enumerate(columns_to_move, start=35): # Iterates through the columns to move and moves them to the 35th position
            df.insert(i, col, df.pop(col)) # Moves the columns

        df = df.drop(columns=["Camera Features"]) # Drops the original Camera Features column

        return df

    def fix_Rear_Video(self, df):
        """""
        This function fixes the Rear Video column. Essentially, it extracts the highest video recording resolution and the frame rate at that resolution
        It also drops the original Rear Video column.

        """""
        def get_video_resolution(x):

            """""
            This function extracts the highest video recording resolution from the string in the Rear Video column.
            If the string contains "no", then "no video" is returned.
            It uses matching to extract the highest video recording resolution.

            """""
            if pd.isnull(x) or x.lower().strip() == "no": # If the value is null or contains "no", then return "no video"
                return "no video"
            
            x = x.split(",")[0] # Splits the string at the comma and takes the first element, highest resolution

            if x.find("8K") != -1: # If the string contains "8K", then return "8K"
                return "8K"
            
            if x.find("4K") != -1: # If the string contains "4K", then return "4K"
                return "4K"

            if x.lower().find("1440p") != -1: # If the string contains "1440p", then return "1440p"
                return "1440p"
            
            if x.lower().find("1080p") != -1: # If the string contains "1080p", then return "1080p"
                return "1080p"

            if x.lower().find("1152p") != -1: # If the string contains "1152p", then return "1152p"
                return "1152p"
            
            if x.lower().find("720p") != -1: # If the string contains "720p", then return "720p"
                return "720p"

            if x.lower().find("480p") != -1: # If the string contains "480p", then return "480p"
                return "480p"

            if x.lower().find("360p") != -1: # If the string contains "360p", then return "360p"
                return "360p"

            if x.lower().find("240p") != -1: # If the string contains "240p", then return "240p"
                return "240p"

            if x.lower().find("320p") != -1: # If the string contains "320p", then return "320p"
                return "320p"

            if x.lower().find("288p") != -1: # If the string contains "288p", then return "288p"
                return "288p"

            if x.lower().find("144p") != -1: # If the string contains "144p", then return "144p"
                return "144p"

            if x.lower().find("120p") != -1: # If the string contains "120p", then return "120p"
                return "120p"

            if x.lower().find("qcif") != -1: # If the string contains "qcif", then return "qcif"
                return "QCIF"

            if x.lower().find("176x144") != -1: # If the string contains "176x144", then return "176x144"
                return "QCIF"

            if x.lower().find("cif") != -1: # If the string contains "cif", then return "cif"
                return "CIF"

            if x.lower().find("352x288") != -1: # If the string contains "352x288", then return "352x288"
                return "CIF"

            if x.lower().find("qvga") != -1: # If the string contains "qvga", then return "qvga"
                return "QVGA"
            
            if x.find("vga") != -1: # If the string contains "vga", then return "vga"
                return "VGA"
            
            res = [i for i in x.split(" ") if i[-2:] == "p@"] # Extracts the highest resolution from the string
            if len(res) > 0: # If the length of the list is greater than 0, then return the highest resolution
                return res[0][:-2] + "p" # Returns the highest resolution
            
            return "unspecified" # Returns "unspecified" for all other rows
            
        def get_video_framerate(x):

            """""
            This function extracts the frame rate at the highest video recording resolution from the string in the Rear Video column.
            If the string contains "no", then "no video" is returned.
            It extracts the framerate before the fps substring.
            """""

            if pd.isnull(x) or x.lower().strip() == "no": # If the value is null or contains "no", then
                return "no video"
            
            x = x.split(",")[0] # Splits the string at the comma and takes the first element, highest resolution
            
            fps = [i for i in x.split("@") if i[-3:] == "fps" or i[-4:] == "fps."] # Extracts the frame rate from the string
            if len(fps) > 0: # If the length of the list is greater than 0, then return the frame rate
                return (fps[0][:-3] if fps[0][-3:] == "fps" else fps[0][:-4]) # Returns the frame rate
            
            return "unspecified"    # Returns "unspecified" for all other rows
        
        df["Rear_Video_Resolution"] = df["Rear Video"].apply(lambda x: get_video_resolution(x)) # Applies the get_video_resolution function to the Rear Video column
        df["Rear_Video_Framerate"] = df["Rear Video"].apply(lambda x: get_video_framerate(x))    # Applies the get_video_framerate function to the Rear Video column

        columns_to_move = ['Rear_Video_Resolution',"Rear_Video_Framerate" ] # Columns to move
        for i, col in enumerate(columns_to_move, start=36): # Iterates through the columns to move and moves them to the 36th and 37th position
            df.insert(i, col, df.pop(col)) # Moves the columns

        df = df.drop(columns=["Rear Video"]) # Drops the original Camera Features column
        
        return df


    def fix_Number_of_Selfie_Cameras(self, df):
        """""
        This function fixes the Number of Selfie Cameras column.
        It extracts the number of selfie cameras from the string in the Number of Selfie Cameras column.
        After extracting the number of selfie cameras, it drops the original Number of Selfie Cameras column.

        """""


        df["Number_of_Selfie_Cameras"] = df["Number of Selfie Cameras"].apply(lambda x: 
            # checks
            3 if not pd.isnull(x) and x.lower().find("triple") != -1 else # If the string contains "triple", then return 3
            2 if not pd.isnull(x) and x.lower().find("dual") != -1 else # If the string contains "dual", then return 2
            1 if not pd.isnull(x) and x.lower().find("single") != -1 else # If the string contains "single", then return 1
            0 # Returns 0 for all other rows

        )

        columns_to_move = ['Number_of_Selfie_Cameras'] # Columns to move
        for i, col in enumerate(columns_to_move, start=39): # Iterates through the columns to move and moves them to the 39th position
            df.insert(i, col, df.pop(col)) # Moves the columns

        df = df.drop(columns=["Number of Selfie Cameras"]) # Drops the original Camera Features column

        return df

    def fix_Selfie_Video(self, df):
        """""
        This function fixes the Rear Video column. Essentially, it extracts the highest video recording resolution and the frame rate at that resolution
        It also drops the original Rear Video column.

        """""
        def get_video_resolution(x):

            """""
            This function extracts the highest video recording resolution from the string in the Rear Video column.
            If the string contains "no", then "no video" is returned.
            It uses matching to extract the highest video recording resolution.

            """""
            if str(x).lower().strip() == "no": # If the value is null or contains "no", then return "no video"
                return "no video"

            if pd.isnull(x): # If the value is null, then return "unspecified"
                return "unspecified"
            
            x = x.split(",")[0] # Splits the string at the comma and takes the first element, highest resolution

            if x.find("8K") != -1: # If the string contains "8K", then return "8K"
                return "8K"
            
            if x.find("4K") != -1: # If the string contains "4K", then return "4K"
                return "4K"

            if x.lower().find("1440p") != -1: # If the string contains "1440p", then return "1440p"
                return "1440p"
            
            if x.lower().find("1080p") != -1: # If the string contains "1080p", then return "1080p"
                return "1080p"

            if x.lower().find("1152p") != -1: # If the string contains "1152p", then return "1152p"
                return "1152p"
            
            if x.lower().find("720p") != -1: # If the string contains "720p", then return "720p"
                return "720p"

            if x.lower().find("480p") != -1: # If the string contains "480p", then return "480p"
                return "480p"

            if x.lower().find("360p") != -1: # If the string contains "360p", then return "360p"
                return "360p"

            if x.lower().find("240p") != -1: # If the string contains "240p", then return "240p"
                return "240p"

            if x.lower().find("320p") != -1: # If the string contains "320p", then return "320p"
                return "320p"

            if x.lower().find("288p") != -1: # If the string contains "288p", then return "288p"
                return "288p"

            if x.lower().find("144p") != -1: # If the string contains "144p", then return "144p"
                return "144p"

            if x.lower().find("120p") != -1: # If the string contains "120p", then return "120p"
                return "120p"

            if x.lower().find("qcif") != -1: # If the string contains "qcif", then return "qcif"
                return "QCIF"

            if x.lower().find("176x144") != -1: # If the string contains "176x144", then return "176x144"
                return "QCIF"

            if x.lower().find("cif") != -1: # If the string contains "cif", then return "cif"
                return "CIF"

            if x.lower().find("352x288") != -1: # If the string contains "352x288", then return "352x288"
                return "CIF"

            if x.lower().find("qvga") != -1: # If the string contains "qvga", then return "qvga"
                return "QVGA"
            
            if x.find("vga") != -1: # If the string contains "vga", then return "vga"
                return "VGA"
            
            res = [i for i in x.split(" ") if i[-2:] == "p@"] # Extracts the highest resolution from the string
            if len(res) > 0: # If the length of the list is greater than 0, then return the highest resolution
                return res[0][:-2] + "p" # Returns the highest resolution
            
            return "unspecified" # Returns "unspecified" for all other rows
            
        def get_video_framerate(x):

            """""
            This function extracts the frame rate at the highest video recording resolution from the string in the Rear Video column.
            If the string contains "no", then "no video" is returned.
            It extracts the framerate before the fps substring.
            """""

            if str(x).lower().strip() == "no": # If the value is null or contains "no", then
                return "no video"

            if pd.isnull(x): # If the value is null, then return "unspecified"
                return "unspecified"
            
            x = x.split(",")[0] # Splits the string at the comma and takes the first element, highest resolution
            
            fps = [i for i in x.split("@") if i[-3:] == "fps" or i[-4:] == "fps."] # Extracts the frame rate from the string
            if len(fps) > 0: # If the length of the list is greater than 0, then return the frame rate
                return (fps[0][:-3] if fps[0][-3:] == "fps" else fps[0][:-4]) # Returns the frame rate
            
            return "unspecified"    # Returns "unspecified" for all other rows
        
        df["Selfie_Video_Resolution"] = df["Selfie Video"].apply(lambda x: get_video_resolution(x)) # Applies the get_video_resolution function to the Rear Video column
        df["Selfie_Video_Framerate"] = df["Selfie Video"].apply(lambda x: get_video_framerate(x))    # Applies the get_video_framerate function to the Rear Video column

        columns_to_move = ['Selfie_Video_Resolution',"Selfie_Video_Framerate" ] # Columns to move
        for i, col in enumerate(columns_to_move, start=40): # Iterates through the columns to move and moves them to the 40th and 41st position
            df.insert(i, col, df.pop(col)) # Moves the columns

        df = df.drop(columns=["Selfie Video"]) # Drops the original Selfie Video column
        
        return df

    def fix_Headphone_Jack(self, df):
        """""
        This function extracts the headphone jack type from the string in the Headphone Jack column.
        It extracts the headphone jack information from the string.

        """""


        df["Headphone_Jack"] = df["Headphone Jack"].apply(lambda x: 
            # checks
            "No" if not pd.isnull(x) and x.lower().find("no") != -1 else # If the string contains "no", then return "no"
            "No" if not pd.isnull(x) and x.lower().find("nO") != -1 else # If the string contains "nO", then return "no"
            "Yes" if not pd.isnull(x) and x.lower().find("yes") != -1 else # If the string contains "yes", then return "yes"
            "unspecified" # Returns "unspecified" for all other rows

        )

        columns_to_move = ['Headphone_Jack'] # Columns to move
        for i, col in enumerate(columns_to_move, start=42): # Iterates through the columns to move and moves them to the 42nd position
            df.insert(i, col, df.pop(col)) # Moves the columns

        df = df.drop(columns=["Headphone Jack"]) # Drops the original Camera Features column

        return df


    def fix_WLAN_Technology(self, df):
        """""
        This function extracts from the string in the WLAN Technology column.
        It creates a new column called WI-FI that contains information if the phone has Wi-Fi or not.
        It drops the original WLAN Technology column.

        """""


        df["WI-FI"] = df["WLAN Technology"].apply(lambda x: 
            # checks
            "No" if not pd.isnull(x) and x.lower().find("no") != -1 else # If the string contains "no", then return "no"
            "Yes" if not pd.isnull(x) and x.lower().find("wi-fi") != -1 else # If the string contains "yes", then return "yes"
            "Yes" if not pd.isnull(x) and x.lower().find("yes") != -1 else # If the string contains "yes", then return "yes"
            "Optional" if not pd.isnull(x) and x.lower().find("optional") != -1 else # If the string contains "optional", then return "optional"
            "unspecified" # Returns "unspecified" for all other rows

        )

        columns_to_move = ['WI-FI'] # Columns to move
        for i, col in enumerate(columns_to_move, start=43): # Iterates through the columns to move and moves them to the 43rd position
            df.insert(i, col, df.pop(col)) # Moves the columns

        df = df.drop(columns=["WLAN Technology"]) # Drops the original Camera Features column

        return df

    def fix_Bluetooth(self, df):
        """""
        This function extracts the bluetooth version from the string in the Bluetooth column.
        It replaces the original Bluetooth column with the new Bluetooth column.

        """""


        df["Bluetooth"] = df["Bluetooth"].apply(lambda x: 
            # checks
            
            
            "5.3" if not pd.isnull(x) and x.lower().find("5.3") != -1 else # If the string contains "5.3", then return "5.3"
            "5.2" if not pd.isnull(x) and x.lower().find("5.2") != -1 else # If the string contains "5.0", then return "5.0"
            "5.1" if not pd.isnull(x) and x.lower().find("5.0") != -1 else # If the string contains "5.1", then return "5.0"
            "5.0" if not pd.isnull(x) and x.lower().find("5.0") != -1 else # If the string contains "5.0", then return "5.0"
            "4.2" if not pd.isnull(x) and x.lower().find("4.2") != -1 else # If the string contains "4.2", then return "4.2"
            "4.1" if not pd.isnull(x) and x.lower().find("4.1") != -1 else # If the string contains "4.1", then return "4.1"
            "4.0" if not pd.isnull(x) and x.lower().find("4.0") != -1 else # If the string contains "4.0", then return "4.0"
            "3.0" if not pd.isnull(x) and x.lower().find("3.0") != -1 else # If the string contains "3.0", then return "3.0"
            "2.1" if not pd.isnull(x) and x.lower().find("2.1") != -1 else # If the string contains "2.1", then return "2.1"
            "2.0" if not pd.isnull(x) and x.lower().find("2.0") != -1 else # If the string contains "2.0", then return "2.0"
            "1.2" if not pd.isnull(x) and x.lower().find("1.2") != -1 else # If the string contains "1.2", then return "1.2"
            "1.1" if not pd.isnull(x) and x.lower().find("1.1") != -1 else # If the string contains "1.1", then return "1.1"
            "1.0" if not pd.isnull(x) and x.lower().find("1.0") != -1 else # If the string contains "1.0", then return "1.0"

            "Yes, unspecified" if not pd.isnull(x) and x.lower().find("yes") != -1 else # If the string contains "yes", then return "yes, unspecified"
            "Yes, unspecified" if not pd.isnull(x) and x.lower().find("bluetooth") != -1 else # If the string contains "bluetooth", then return "yes, unspecified"
            "No" if not pd.isnull(x) and x.lower().find("no") != -1 else # If the string contains "no", then return "no"
            "No" # Returns "No" for all other rows

        )

        return df


    def fix_NFC(self, df):
        """""
        This function extracts whther NFC is present or not from the string in the NFC column.
        It differentiates between "Yes (Market/Model/Use Case Dependent)" and "Yes".
        It replaces the original NFC column with the new NFC column.

        """""


        df["NFC"] = df["NFC"].apply(lambda x: 
            # checks
            
            
            "Yes (Market/Model/Use Case Dependent)" if not pd.isnull(x) and x.lower().find("optional") != -1 else # If the string contains "optional", then return "Yes (Market/Model/Use Case Dependent)"
            "Yes (Market/Model/Use Case Dependent)" if not pd.isnull(x) and x.lower().find("option") != -1 else # If the string contains "option", then return "Yes (Market/Model/Use Case Dependent)"
            "Yes (Market/Model/Use Case Dependent)" if not pd.isnull(x) and x.lower().find("only") != -1 else # If the string contains "only", then return "Yes (Market/Model/Use Case Dependent)"
            "Yes (Market/Model/Use Case Dependent)" if not pd.isnull(x) and x.lower().find("dependent") != -1 else # If the string contains "dependent", then return "Yes (Market/Model/Use Case Dependent)"
            "Yes (Market/Model/Use Case Dependent)" if not pd.isnull(x) and x.lower().find("specific") != -1 else # If the string contains "specific", then return "Yes (Market/Model/Use Case Dependent)"
            "Yes (Market/Model/Use Case Dependent)" if not pd.isnull(x) and x.lower().find("model") != -1 else # If the string contains "model", then return "Yes (Market/Model/Use Case Dependent)"
            "Yes (Market/Model/Use Case Dependent)" if not pd.isnull(x) and x.lower().find("excl") != -1 else # If the string contains "excl", then return "Yes (Market/Model/Use Case Dependent)"

            "Yes" if not pd.isnull(x) and x.lower().find("yes") != -1 else # If the string contains "yes", then return "yes"
            "Yes" if not pd.isnull(x) and x.lower().find("nfc") != -1 else # If the string contains "nfc", then return "Yes, unspecified"
            "No" if not pd.isnull(x) and x.lower().find("no") != -1 else # If the string contains "no", then return "no"
            "unspecified" if not pd.isnull(x) and x.lower().find("unspecified") != -1 else # If the string contains "unspecified", then return "unspecified"
            "No" # Returns "No" for all other rowss

        )

        return df

        
    def fix_Radio(self, df):
        """""
        This function extracts whther Radio is present or not from the string in the Radio column.
        It differentiates between "Yes (Market/Model/Software Dependent)" and "Yes".

        """""


        df["Radio"] = df["Radio"].apply(lambda x: 
            # checks
            
            
            "Yes (Market/Model/Software Dependent)" if not pd.isnull(x) and x.lower().find("optional") != -1 else # If the string contains "optional", then return "Yes (Market/Model/Software Dependent)"
            "Yes (Market/Model/Software Dependent)" if not pd.isnull(x) and x.lower().find("only") != -1 else # If the string contains "only", then return "Yes (Market/Model/Software Dependent)"
            "Yes (Market/Model/Software Dependent)" if not pd.isnull(x) and x.lower().find("markets") != -1 else # If the string contains "markets", then return "Yes (Market/Model/Software Dependent)"
            "Yes (Market/Model/Software Dependent)" if not pd.isnull(x) and x.lower().find("dependent") != -1 else # If the string contains "dependent", then return "Yes (Market/Model/Software Dependent)"
            "Yes (Market/Model/Software Dependent)" if not pd.isnull(x) and x.lower().find("specific") != -1 else # If the string contains "specific", then return "Yes (Market/Model/Software Dependent)"
            "Yes (Market/Model/Software Dependent)" if not pd.isnull(x) and x.lower().find("software") != -1 else # If the string contains "software", then return "Yes (Market/Model/Software Dependent)"

            "Yes" if not pd.isnull(x) and x.lower().find("yes") != -1 else # If the string contains "yes", then return "yes"
            "Yes" if not pd.isnull(x) and x.lower().find("fm") != -1 else # If the string contains "nfc", then return "Yes, unspecified"
            "Yes" if not pd.isnull(x) and x.lower().find("radio") != -1 else # If the string contains "nfc", then return "Yes, unspecified"
            "Yes" if not pd.isnull(x) and x.lower().find("stereo") != -1 else # If the string contains "nfc", then return "Yes, unspecified"

            "No" if not pd.isnull(x) and x.lower().find("no") != -1 else # If the string contains "no", then return "no"
            "unspecified" if not pd.isnull(x) and x.lower().find("unspecified") != -1 else # If the string contains "unspecified", then return "unspecified"
            "No" # Returns "No" for all other rowss

        )

        return df

    def fix_USB(self, df):
        """""
        This function extracts the USB connector type from the string in the USB column.
        It also extracts the usb version from the string in the USB column.
        It returns this information in two separate columns.
        It drops the USB column.

        """""


        df["USB_Connector"] = df["USB"].apply(lambda x: 
            # checks
            
            
            "Type-C" if not pd.isnull(x) and x.lower().find("type-c") != -1 else # if the string contains "type-c", then return "type-c"
            "Type-C" if not pd.isnull(x) and x.lower().find("usb-c") != -1 else # if the string contains "usb-c", then return "type-c"
            "miniUSB" if not pd.isnull(x) and x.lower().find("mini") != -1 else # if the string contains "mini", then return "miniUSB"
            "microUSB" if not pd.isnull(x) and x.lower().find("micro") != -1 else # if the string contains "micro", then return "microUSB"
            "Pop-Port" if not pd.isnull(x) and x.lower().find("pop-port") != -1 else # if the string contains "pop", then return "Pop-Port"
            "Pop-Port" if not pd.isnull(x) and x.lower().find("pop") != -1 else # if the string contains "pop", then return "Pop-Port"
            "USB" if not pd.isnull(x) and x.lower().find("usb") != -1 else # if the string contains "usb", then return "USB"
            "Proprietary" if not pd.isnull(x) and x.lower().find("proprietary") != -1 else # if the string contains "proprietary", then return "Proprietary"

            
            "No" if not pd.isnull(x) and x.lower().find("no") != -1 else # If the string contains "no", then return "no"

            "unspecified" if not pd.isnull(x) and x.lower().find("yes") != -1 else # If the string contains "yes", then return "unspecified"
            "unspecified" if not pd.isnull(x) and x.lower().find("unspecified") != -1 else # If the string contains "unspecified", then return "unspecified"
            "unspecified" # Returns "unspecified" for all other rows

        )

        df["USB_Version"] = df["USB"].apply(lambda x: 
            # checks
            
            
            "1.1" if not pd.isnull(x) and x.lower().find("1.1") != -1 else # if the string contains "1.1", then return "1.1"
            "2.0" if not pd.isnull(x) and x.lower().find("2.0") != -1 else # if the string contains "2.0", then return "2.0"
            "3.0" if not pd.isnull(x) and x.lower().find("3.0") != -1 else # if the string contains "3.0", then return "3.0"
            "3.1" if not pd.isnull(x) and x.lower().find("3.1") != -1 else # if the string contains "3.1", then return "3.1"
            "3.2" if not pd.isnull(x) and x.lower().find("3.2") != -1 else # if the string contains "3.2", then return "3.2
            "4.0" if not pd.isnull(x) and x.lower().find("4") != -1 else # if the string contains "4.0", then return "4.0"
            "4.1" if not pd.isnull(x) and x.lower().find("4.1") != -1 else # if the string contains "4.1", then return "4.1"
            "4.2" if not pd.isnull(x) and x.lower().find("4.2") != -1 else # if the string contains "4.2", then return "4.2"

            
        
            "No" if not pd.isnull(x) and x.lower().find("no") != -1 else # If the string contains "no", then return "no"

            "unspecified" if not pd.isnull(x) and x.lower().find("yes") != -1 else # If the string contains "yes", then return "yes"
            "unspecified" if not pd.isnull(x) and x.lower().find("unspecified") != -1 else # If the string contains "unspecified", then return "unspecified"
            
            "unspecified" #  Returns "unspecified" for all other rowss

        )

        columns_to_move = ['USB_Connector', 'USB_Version'] # Columns to move
        for i, col in enumerate(columns_to_move, start=47): # Iterates through the columns to move and moves them to the 47th and 48th positions
            df.insert(i, col, df.pop(col)) # Moves the columns

        df = df.drop(columns=["USB"]) # Drops the original USB column

        return df


    def fix_Sensors(self, df):
        """""
        This function fixes the Sensors column. It establishes whether or not a phone has a biometric sensor and what type of sensor it is.
        It also extracts the information about the sensor technology and its placement.
        It returns this information in 4 new columns and drops the original Sensors column.

        """""


        df["Biometric_Sensor"] = df["Sensors"].apply(lambda x: 
            # checks
            
            
            "Yes" if not pd.isnull(x) and x.lower().find("fingerprint") != -1 else # if the string contains "fingerprint", then return "Yes"
            "Yes" if not pd.isnull(x) and x.lower().find("iris") != -1 else # if the string contains "iris", then return "Yes"
            "Yes" if not pd.isnull(x) and x.lower().find("face") != -1 else # if the string contains "face", then return "Yes"
            "No" if not pd.isnull(x) and x.lower().find("no") != -1 else # If the string contains "no", then return "no"

            "unspecified" if not pd.isnull(x) and x.lower().find("yes") != -1 else # If the string contains "yes", then return "unspecified"
            
            "No" # Returns "No" for all other rows

        )

        df["Biometric_Sensor_Type"] = df["Sensors"].apply(lambda x: 
            # checks
            
            
            "Fingerprint" if not pd.isnull(x) and x.lower().find("fingerprint") != -1 else # if the string contains "fingerprint", then return "Fingerprint"
            "Iris" if not pd.isnull(x) and x.lower().find("iris") != -1 else # if the string contains "iris", then return "Iris"
            "Face" if not pd.isnull(x) and x.lower().find("face") != -1 else # if the string contains "face", then return "Face"

            "No" if not pd.isnull(x) and x.lower().find("no") != -1 else # If the string contains "no", then return "no"
            "unspecified" if not pd.isnull(x) and x.lower().find("yes") != -1 else # If the string contains "yes", then return "yes"
            
            "No" #  Returns "unspecified" for all other rowss

        )

        df["Biometric_Sensor_Technology"] = df["Sensors"].apply(lambda x:
            # checks
            "Infrared" if not pd.isnull(x) and x.lower().find("infrared") != -1 else # if the string contains "infrared", then return "Infrared"
            "Optical" if not pd.isnull(x) and x.lower().find("optical") != -1 else # if the string contains "optical", then return "Optical"
            "Ultrasonic" if not pd.isnull(x) and x.lower().find("ultrasonic") != -1 else # if the string contains "ultrasonic", then return "Ultrasonic"
            "RFID" if not pd.isnull(x) and x.lower().find("rfid") != -1 else # if the string contains "rfid", then return "RFID"
            "Capacitive" if not pd.isnull(x) and x.lower().find("capacitive") != -1 else # if the string contains "capacitive", then return "Capacitive"

            "No" if not pd.isnull(x) and x.lower().find("no") != -1 else # If the string contains "no", then return "no"
            "unspecified" if not pd.isnull(x) and x.lower().find("yes") != -1 else # If the string contains "yes", then return "yes"
            "No"
        )

        df["Biometric_Sensor_Location"] = df["Sensors"].apply(lambda x:
            # checks
            "Top-mounted" if not pd.isnull(x) and x.lower().find("top-mounted") != -1 else # if the string contains "top", then return "Top-mounted"
            "Side-mounted" if not pd.isnull(x) and x.lower().find("side-mounted") != -1 else # if the string contains "side", then return "Side-mounted"
            "Rear-mounted" if not pd.isnull(x) and x.lower().find("rear-mounted") != -1 else # if the string contains "rear", then return "Rear-mounted"
            "Front-mounted" if not pd.isnull(x) and x.lower().find("front-mounted") != -1 else # if the string contains "front", then return "Front-mounted"
            "Under-display" if not pd.isnull(x) and x.lower().find("under display") != -1 else # if the string contains "under-display", then return "Under-display"
            "Front-facing" if not pd.isnull(x) and x.lower().find("face") != -1 else # if the string contains "front-facing", then return "Front-facing"
            "No" if not pd.isnull(x) and x.lower().find("no") != -1 else # If the string contains "no", then return "no"
            "unspecified" if not pd.isnull(x) and x.lower().find("yes") != -1 else # If the string contains "yes", then return "yes"
            "No"
        )

        
        columns_to_move = ['Biometric_Sensor', 'Biometric_Sensor_Type', 'Biometric_Sensor_Technology', 'Biometric_Sensor_Location'] # Columns to move
        for i, col in enumerate(columns_to_move, start=49): # Iterates through the columns to move and moves them to the 49th to the 52nd positions
            df.insert(i, col, df.pop(col)) # Moves the columns

        df = df.drop(columns=["Sensors"]) # Drops the original Sensors column
        
        return df


    def fix_UWB(self, df):
        """""
        This function fixes the UWB column. It checks if the string contains "uwb" and returns "Yes" if it does, and "No" if it doesn't.
        It replaces the original UWB column with the new one that shows "Yes" or "No" for each row.

        """""


        df["UWB"] = df["UWB"].apply(lambda x: 
            # checks
            
            "Yes" if not pd.isnull(x) and x.lower().find("uwb") != -1 else # if the string contains "uwb", then return "Yes"
            "No" # Returns "No" for all other rows

        )
        return df


    def fix_Battery(self, df):

        """""
        This function fixes the Battery column. It checks if the string contains "non-removable" and returns "No" if it does, and "Yes" if it doesn't.
        It also extracts the battery size from the string and returns it as an integer.
        It also extracts the battery type from the string and returns it as a string.
        It drops the original Battery column and replaces it with the new columns.

        """""
        

        def extract_battery_size(battery_string):
            battery_string = str(battery_string)
            match = re.search(r'\b\d+\b', battery_string)
            if match:
                return int(match.group(0))
            else:
                return 'unspecified'


        df["Removable"] = df["Battery"].apply(lambda x:  # Creates a new column called "Removable"
            # checks
            "No" if not pd.isnull(x) and x.lower().find("non-removable") != -1 else # if the string contains "non-removable", then return "No"
            "Yes" if not pd.isnull(x) and x.lower().find("removable") != -1 else # if the string contains "uwb", then return "Yes"
            "unspecified" # Returns "No" for all other rows

        )

        df["Battery_Type"] = df["Battery"].apply(lambda x:
            # checks
            "Li-Ion" if not pd.isnull(x) and x.lower().find("li-ion") != -1 else # if the string contains "li-ion", then return "Li-Ion"
            "Li-Po" if not pd.isnull(x) and x.lower().find("li-po") != -1 else # if the string contains "li-po", then return "Li-Po"
            "Li-Po" if not pd.isnull(x) and x.lower().find("li-polymer") != -1 else # if the string contains "li-polymer", then return "Li-Polymer"
            "unspecified" # Returns "No" for all other rows

        )

        df["Battery_Capacity"] = df["Battery"].apply(extract_battery_size)

        columns_to_move = ['Removable', 'Battery_Type', 'Battery_Capacity'] # Columns to move
        for i, col in enumerate(columns_to_move, start=55): # Iterates through the columns to move and moves them to the 55th to the 57th positions
            df.insert(i, col, df.pop(col)) # Moves the columns

        df = df.drop(columns=["Battery"]) # Drops the original Battery column
        
        return df

    def fix_Charging(self, df):

        """""
        This function fixes the Charging column. It checks if the string contains "wireless" and returns "Yes" if it does, and "No" if it doesn't.
        It also extracts the charging speed from the string and returns it as an integer.
        It also extracts the reverse charging speed from the string and returns it as an integer.
        It drops the original Charging column and replaces it with the new columns.

        """""
        def extract_charging_speed(charging_string):
            """""


            """""

            if pd.isnull(charging_string):
                return 'unspecified'
            match = re.findall(r'\b\d+W', charging_string)
            if match:
                return max(int(x.strip("W")) for x in match)
            else:
                return 'unspecified'

        df["Wireless_Charging"] = df["Charging"].apply(lambda x:  # Creates a new column called "Removable"
            # checks
            "Yes" if not pd.isnull(x) and x.lower().find("wireless") != -1 else # if the string contains "non-removable", then return "No"
            "unspecified" # Returns "No" for all other rows

        )

        df["Reverse_Charging"] = df["Charging"].apply(lambda x:
            # checks
            "Yes" if not pd.isnull(x) and x.lower().find("reverse") != -1 else # if the string contains "li-ion", then return "Li-Ion"
            "unspecified" # Returns "No" for all other rows

        )

        df["Charging_Speed"] = df["Charging"].apply(extract_charging_speed)

        columns_to_move = ['Charging_Speed', 'Wireless_Charging', 'Reverse_Charging'] # Columns to move
        for i, col in enumerate(columns_to_move, start=58): # Iterates through the columns to move and moves them to the 58th to the 60th positions
            df.insert(i, col, df.pop(col)) # Moves the columns

        df = df.drop(columns=["Charging"]) # Drops the original Battery column
        
        return df

    def fix_Colors(self, df):

        """""
        This function fixes the Colors column. It counts the number of color options in the string and returns it as an integer.

        """""


        def count_color_options(colors): # Counts the number of color options in the string and returns it as an integer.
            if pd.isna(colors): # If the string is empty,
                return 0 # Return 0
            return len(colors.split(",")) # Otherwise, return the number of color options


        df['Colors'] = df['Colors'].apply(count_color_options) # Applies the count_color_options function to the Colors column

        return df


    def fix_Battery_Life(self, df):

        """""
        This function fixes the Battery Life column. It extracts the endurance from the string and returns it as an integer.
        It drops the original Battery Life column and replaces it with the new column.

        """"" 

        def extract_endurance(charging_string):
            """""
            This function extracts the endurance from the string and returns it as an integer.
            It uses pattern matching to find the endurance in the string.

            """""

            if pd.isnull(charging_string): # If the string is empty,
                return 'untested'
            match = re.findall(r'\b\d+h', charging_string) # Finds the endurance in the string
            if match: # If the string contains the endurance,
                return max(int(x.strip("h")) for x in match) # Return the endurance
            else:
                return 'untested' # Otherwise, return "untested"

        df["Battery_Life"] = df["Battery Life"].apply(extract_endurance) # Creates a new column called "Battery_Life" and applies the extract_endurance function to it

        columns_to_move = ['Battery_Life'] # Columns to move
        for i, col in enumerate(columns_to_move, start=63): # Iterates through the columns to move and moves them to the 63rd position
            df.insert(i, col, df.pop(col)) # Moves the columns

        df = df.drop(columns=["Battery Life"]) # Drops the original Battery column

        return df


    def fix_Internal_Storage(self, df):

        """""
        This function fixes the Internal Storage column. It extracts the storage capacity (ROM) and the RAM from the string.
        It returns these as integers in GB in their own respective columns.
        This function's logic is based on the logic that the vast majority of phones have a higher storage capacity than RAM.
        It therefore extracts the higher of the two values and returns it as the storage capacity, and the lower of the two values as the RAM.
        NB: there are some fringe cases where this logic does not work (more RAM than ROM) giving wrong values, but these are very rare
        Work can be done to improve this function to fix these cases.

        """""

        def extract_storage(internal_storage_string):

            """""

            This function extracts the storage capacity (ROM) from the string.
            It takes the highest value after pattern matching

            """""

            if pd.isnull(internal_storage_string): # If the string is empty,
                return 'unspecified'
            
            internal_storage_string = re.sub(r'\([^)]*\)', '', internal_storage_string) # remove anything inside brackets
            match = re.findall(r'\b\d+GB', internal_storage_string) + re.findall(r'\b\d+MB', internal_storage_string) # Finds the capacity in the string
            if match: # If the string contains the capacity,
                match = [int(x.strip("GB")) if "GB" in x else int(x.strip("MB"))/1000 for x in match] # Convert the capacity to GB
                if len(match) > 1:
                    return max(match) # Return the max capacity
                else:
                    return match[0] # Otherwise, return the capacity
            else:
                return 'unspecified' # Otherwise, return "unspecified"

        def extract_ram(internal_storage_string):

            """""

            This function extracts the storage capacity (ROM) from the string.
            It takes the highest value after pattern matching

            """""
        
            if pd.isnull(internal_storage_string): # If the string is empty,
                return 'unspecified'
            
            match = re.findall(r'\b\d+GB', internal_storage_string) + re.findall(r'\b\d+MB', internal_storage_string) # Finds the capacity in the string
            if match: # If the string contains the capacity,
                match = [int(x.strip("GB")) if "GB" in x else int(x.strip("MB"))/1000 for x in match] # Convert the capacity to GB
                if len(match) > 1:
                    return min(match) # Return the min capacity
                else:
                    return 'unspecified'
            else:
                return 'unspecified' # Otherwise, return "unspecified"


        df["ROM"] = df["Internal Storage"].apply(extract_storage) # Creates a new column called "Internal_Storage" and applies the extract_storage function to it
        df["RAM"] = df["Internal Storage"].apply(extract_ram) # Creates a new column called "RAM" and applies the extract_ram function to it

        columns_to_move = ['ROM', 'RAM'] # Columns to move
        for i, col in enumerate(columns_to_move, start=33): # Iterates through the columns to move and moves them to the 33rd and 34th positions
            df.insert(i, col, df.pop(col)) # Moves the columns

        df = df.drop(columns=["Internal Storage"]) # Drops the original Internal Storage column

        return df


    def fix_Storage_Type(self, df):
        """""
        This function fixes the Storage Type column. It extracts the storage type (UFS or eMMC) from the string.
        It also returns the Version of the storage type as a float.
        It drops the original Storage Type column.

        """""

        def extract_UFS_eMMC(x): # Extracts the storage type from the string
            if pd.notnull(x): # If the string is not empty,
                match = re.findall(r'\b(UFS|eMMC)\s(\d+\.\d+)', str(x)) # Finds the storage type and version in the string
                if match: # If the string contains the storage type and version,
                    return match[0][1] # Return the version
            return 'unspecified' # Otherwise, return "unspecified"


        df["Storage_Type_Version"] = df['Storage Type'].apply(extract_UFS_eMMC) # Creates a new column called "Storage_Type_Version" and applies the extract_UFS_eMMC function to it

        df["Storage_Type"] = df["Storage Type"].apply(lambda x: # Creates a new column called "Storage_Type" and applies the following lambda function to it
            # checks
            "eMMC" if not pd.isnull(x) and x.lower().find("emmc") != -1 else # If the string contains "eMMC", return "eMMC"
            "UFS" if not pd.isnull(x) and x.lower().find("ufs") != -1 else # If the string contains "UFS", return "UFS"
            "unspecified" # Otherwise, return "unspecified"
        )

        columns_to_move = ['Storage_Type_Version', 'Storage_Type'] # Columns to move
        for i, col in enumerate(columns_to_move, start=35): # Iterates through the columns to move and moves them to the 35th and 36th positions
            df.insert(i, col, df.pop(col)) # Moves the columns

        df = df.drop(columns=["Storage Type"]) # Drops the original Storage Type column

        return df


        
    def fix_Loudspeaker(self, df):

        """""


        This function fixes the Loudspeaker column. It extracts the 3 values of dB for Voice, Noise and Ring.
        It returns the values as floats, and denotes "untested" to the values that are not available.
        It drops the original Loudspeaker column.

        """""


        extract_num = lambda x: int(re.findall(r'\d+', x)[0]) if re.search(r'\d+', x) else None # Extracts the number from the string

        df['Loudspeaker'] = df['Loudspeaker'].apply(lambda x: np.nan if 'LUFS' in str(x) else x) # Replaces the values with "LUFS" with NaN

        df['Loudspeaker_Voice'] = df['Loudspeaker'].apply(lambda x: x.split('/')[0] if pd.notnull(x) else x) # Splits the string into 3 parts and extracts the first part
        df['Loudspeaker_Noise'] = df['Loudspeaker'].apply(lambda x: x.split('/')[1] if pd.notnull(x) else x) # Splits the string into 3 parts and extracts the second part
        df['Loudspeaker_Ring'] = df['Loudspeaker'].apply(lambda x: x.split('/')[-1] if pd.notnull(x) else x) # Splits the string into 3 parts and extracts the third part

        df['Loudspeaker_Voice'] = df['Loudspeaker_Voice'].astype(str).apply(extract_num) # extracts the values and converts them to float
        df['Loudspeaker_Noise'] = df['Loudspeaker_Noise'].astype(str).apply(extract_num) # extracts the values and converts them to float
        df['Loudspeaker_Ring'] = df['Loudspeaker_Ring'].astype(str).apply(extract_num) # extracts the values and converts them to float

        df[['Loudspeaker_Voice', 'Loudspeaker_Noise', 'Loudspeaker_Ring']] = df[['Loudspeaker_Voice', 'Loudspeaker_Noise', 'Loudspeaker_Ring']].fillna('untested') # Replaces the NaN values with "untested"

        columns_to_move = ['Loudspeaker_Voice', 'Loudspeaker_Noise', 'Loudspeaker_Ring'] # Columns to move
        for i, col in enumerate(columns_to_move, start=64): # Iterates through the columns to move and moves them to the 64th, 65th and 66th positions

            df.insert(i, col, df.pop(col)) # Moves the columns

        df = df.drop(columns=["Loudspeaker"]) # Drops the original Loudspeaker column

        
        return df

    def fix_price_and_approx_price(self, df):

        """""
        This function fixes the Price and Approx Price columns. It extracts the price from the string and converts it to float.
        It returns the values as rounded floats, and denotes "unspecified" to the values that are not available.
        It gives the value in EUR and converts other currencies to EUR using the exchange rate on 08/02/2023.

        """""

        def extract_from_approx_price(price):

            """""
            This function extracts the price from the Approx Price column.
            It uses pattern matching to extract the price from the string.
            It returns the values as rounded floats, and denotes nan to the values that are not available.
            It gives the value in EUR and converts other currencies to EUR using the exchange rate on 08/02/2023.
            """""

            pattern = r"About (\d+\.\d+|\d+) EUR" # Pattern to match
            match = re.search(pattern, price) # Searches for the pattern in the string
            if match: # If the pattern is found, returns the value
                return round(int(match.group(1))) # Returns the value as rounded float
            pattern = r"About (\d+\.\d+|\d+) INR" # Pattern to match
            match = re.search(pattern, price) # Searches for the pattern in the string
            if match: # If the pattern is found, returns the value
                return round(int(match.group(1)) * 0.011) # Returns the value as rounded float
            return None # If the pattern is not found, returns None


        def extract_from_price(price_str):

            """""
            This function extracts the price from the Price column.
            It uses pattern matching to extract the price from the string.
            It returns the values as rounded floats, and denotes nan to the values that are not available.
            It gives the value in EUR and converts other currencies to EUR using the exchange rate on 08/02/2023.

            """""

            if not isinstance(price_str, str): # If the value is not a string, returns nan
                return price_str # Returns nan
            euros = re.findall(r'€\s*(\d+(?:\.\d+)?)', price_str) # Finds the pattern in the string
            if euros: # If the pattern is found, returns the value
                return round(float(euros[0])) # Returns the value as rounded float
            dollars = re.findall(r'\$\s*(\d+(?:\.\d+)?)', price_str) # Finds the pattern in the string
            if dollars: # If the pattern is found, returns the value
                return round(float(dollars[0]) * 0.93) # Returns the value as rounded float
            inr = re.findall(r'₹\s*(\d+(?:,\d+)?)', price_str) # Finds the pattern in the string
            if inr: # If the pattern is found, returns the value
                return round(float(inr[0].replace(',', '')) * 0.011) # Returns the value as rounded float
            pounds = re.findall(r'£\s*(\d+(?:\.\d+)?)', price_str) # Finds the pattern in the string
            if pounds: # If the pattern is found, returns the value
                return round(float(pounds[0]) * 1.13) # Returns the value as rounded float
            return float('nan') # If the pattern is not found, returns nan


        df["Approx Price"] = df["Approx Price"].apply(lambda x: extract_from_approx_price(x) if pd.notna(x) else None) # Extracts the price from the string and converts it to float
        df['Price'] = df['Price'].apply(extract_from_price) # Extracts the price from the string and converts it to float

        df['Price'] = df['Approx Price'].combine_first(df['Price']) # Combines the two columns
        df['Price'] = df['Price'].fillna("unspecified") # Replaces the NaN values with "unspecified"

        columns_to_move = ['Price'] # Columns to move
        for i, col in enumerate(columns_to_move, start=68): # Iterates through the columns to move and moves them to the 67th position

            df.insert(i, col, df.pop(col)) # Moves the columns

        df = df.drop(columns=["Approx Price"]) # Drops the original Price and Approx Price columns

        return df


    def clean(self):

        cleaned_df = self.df
        for func in self.cleaning_functions:
            cleaned_df = func(cleaned_df)

        return cleaned_df
        