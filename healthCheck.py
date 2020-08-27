import glob
import os
import pandas

def main():

    ##########################################################################
    root = 'C:\\Users\\Tyler\\Documents\\MK\\Mediaroom Health Checks\\Sasktel'
    ##########################################################################

    newRoot = os.path.join(root + "\\MachineTypes")
    listofMachineTypes = os.listdir(newRoot)

    #creates directory for the output (CSV) files
    newDirectory = 'Reports'
    newDirectoryPath = root + '\\' + newDirectory
    os.mkdir(newDirectoryPath)

    #for each folder in the root directory (represents one machine type)
    for machineType in listofMachineTypes:

        machinePath = os.path.join(newRoot, machineType)
        listofMachines = os.listdir(machinePath)

        #for each machine in the machine type folder (ex CGSG101 in folder CGSG)
        for machine in listofMachines:

            #ERROR WHEN USING MODIN TO CREATE DF
            finalFrame = pandas.DataFrame()
            dirPath = os.path.join(machinePath, machine)

            #for each log file in the folder
            for filepath in glob.glob(os.path.join(dirPath, "*log")):

                #for debugging
                print(filepath)
                
                #READS IN THE FIELDS FROM THE COMMENTS
                first4Rows = pd.read_csv(filepath, nrows=4, names=["comments"])
                columnNames = first4Rows.iloc[3]["comments"]
                columnNames = columnNames.split(" ")
                columnNames.pop(0)

                #keep PANDS, will have memory error if use MODIN
                frame = pandas.read_csv(filepath, sep = ' ', comment = '#', engine = 'python', names=columnNames, encoding= "utf-8")

                #drop all irrelevant columns
                frame.drop(frame.columns.difference(['date','time', 'cs-uri-stem', 'sc-status']), 1, inplace= True)

                #creates new column on dataframe for machinetype
                frame['MachineType'] = machineType
                
                #uses first two digits of time, for grouping by hour
                frame['time'] = [s[:2] for s in frame['time']]

                #possible fix to save memory
                frame[['time', 'sc-status']].astype('int32')

                #for grouping URIs, specific to CGSG, possibly results in attribute error
                if 'CGSG' in machineType:
                    try:
                        frame.loc[frame['cs-uri-stem'].str.contains('/ListingsRest'), 'cs-uri-stem'] = '/ListingsRestService'
                        frame.loc[frame['cs-uri-stem'].str.contains('/ClientUpgrades'), 'cs-uri-stem'] = '/ClientUpgrades'
                    except AttributeError:
                        pass

                #joins the edited frame to the frame for that machine
                finalFrame = pd.concat([finalFrame, frame], ignore_index=True)

            p = os.path.basename(dirPath)
            csvFileName = os.path.join(newDirectoryPath + "\\" + p + "." + "csv")
            print("exporting csv " + csvFileName)

            #exports each machine's data to CSV, potentially keep chunksize
            try:
                finalFrame.to_csv(csvFileName, index=False, chunksize=100000)
            except TypeError:
                print("Serializeable Type Error")
                pass
            


if __name__ == "__main__":
    import modin.pandas as pd
    main()