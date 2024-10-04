# import os
# import pandas as pd
# import numpy as np

# # --- Misc fast libraries
# # from openfast_toolbox.io.fast_wind_file import FASTWndFile
# from pyFAST.input_output.fast_wind_file import FASTWndFile


# def createStepWind(
#     filename, WSstep=1, WSmin=3, WSmax=25, tstep=100, dt=0.1, tmin=0, tmax=999
# ):
#     """
#     Creates a step wind file for wind turbine simulations.

#     This function generates a wind file with step changes in wind speed at regular intervals,
#     useful for steady-state or other types of wind turbine simulations.

#     Parameters:
#         filename (str): Path to the output wind file.
#         WSstep (float): Step size for wind speed changes. Default is 1.
#         WSmin (float): Minimum wind speed. Default is 3.
#         WSmax (float): Maximum wind speed. Default is 25.
#         tstep (int): Time step in seconds for each wind speed step. Default is 100.
#         dt (float): Small time delta before the next step change. Default is 0.1.
#         tmin (float): Starting time of the wind file. Default is 0.
#         tmax (float): Maximum time of the wind file. Default is 999.

#     Returns:
#         float: The final time of the wind file.

#     The function checks if the wind file already exists to avoid overwriting. If not,
#     it creates a new file with specified parameters.
#     """

#     # Assuming FASTWndFile() is defined elsewhere
#     f = FASTWndFile()

#     Steps = np.arange(WSmin, WSmax + WSstep, WSstep)
#     nCol = len(f.colNames)
#     nRow = len(Steps) * 2
#     M = np.zeros((nRow, nCol))
#     M[0, 0] = tmin
#     M[0, 1] = WSmin

#     for i, s in enumerate(Steps[:-1]):
#         M[2 * i + 1, 0] = tmin + (i + 1) * tstep - dt
#         M[2 * i + 2, 0] = tmin + (i + 1) * tstep
#         M[2 * i + 1, 1] = Steps[i]
#         M[2 * i + 2, 1] = Steps[i + 1]

#     final_time = tmin + len(Steps) * tstep
#     M[-2, 0] = final_time - dt
#     M[-1, 0] = max(tmax, final_time)
#     M[-2, 1] = WSmax
#     M[-1, 1] = WSmax

#     f.data = pd.DataFrame(data=M, columns=f.colNames)
#     # Check if the file already exists
#     if os.path.exists(filename):
#         print(f"StepWind File {filename} already exists. Skipping creation.")
#         return float(max(tmax, final_time))
#     f.write(filename)

#     return float(max(tmax, final_time))


# if __name__ == "__main__":
#     # Example usage
#     scriptDir = os.path.dirname(__file__)

#     # - Create Stepwind
#     # stepwindFile = 'StepWind_3-25_05.wnd'
#     TMax = createStepWind(
#         r"D:\data\47_ruck\61_MannBox\Mann_Box\InitalCondtions/StepWind_3-25_WSstep_0p5_tstep_120.wnd",
#         WSstep=0.5,
#         WSmin=3,
#         WSmax=25,
#         tstep=120,
#         dt=0.1,
#     )
#     # TMax = createStepWind(os.path.join(scriptDir, f'../InitialConditions/{stepwindFile}'),WSstep=0.5,WSmin=3,WSmax=25,tstep=120)
#     print(TMax)


# """Import modules"""

# import os
# import matplotlib.pyplot as plt
# import numpy as np
# import pandas as pd
# from scipy import stats
# import csv

# # from openfast_toolbox.io import FASTInputFile
# # from openfast_toolbox.io import FASTOutputFile

# from pyFAST.input_output import FASTInputFile
# from pyFAST.input_output import FASTOutputFile


# import time


# def Initial_Conditions(filename_IC, windspeed):
#     """
#     Processes a FAST output file to filter and compute initial conditions for specified wind speeds.

#     This function reads a FAST output file, filters the data based on specified time intervals,
#     and computes the initial conditions for a given range of wind speeds. These conditions
#     include rotor speed, blade pitch angles, blade tip deflections, and tower top displacements.

#     Parameters:
#         filename_IC (str): The path to the FAST output file to be processed.
#         windspeed (numpy.ndarray): An array of wind speeds for which initial conditions are to be computed.

#     Returns:
#         tuple: A tuple containing lists of computed initial conditions:
#                - RPM (list): Rotor speeds (in rpm) for each wind speed.
#                - PITCH_1, PITCH_2, PITCH_3 (lists): Pitch angles (in degrees) for blades 1, 2, and 3, respectively.
#                - OoPDefl (list): Averaged out-of-plane blade-tip displacements (in meters) for all blades.
#                - IPDefl (list): Averaged in-plane blade-tip deflections (in meters) for all blades.
#                - TTDspFA (list): Fore-aft tower-top displacements (in meters).
#                - TTDspSS (list): Side-to-side tower-top displacements (in meters).

#     Example:
#         ws_start = 4
#         ws_stop = 24
#         ws_step = 2
#         windspeed = np.arange(ws_start, ws_stop + ws_step, ws_step)
#         filename_IC = '../results/Basecase/SteadyState.outb'
#         RPM, PITCH_1, PITCH_2, PITCH_3, OoPDefl, IPDefl, TTDspFA, TTDspSS = Initial_Conditions(filename_IC, windspeed)
#     """
#     # Get current directory so this script can be called from any location
#     scriptDir = os.path.dirname(__file__)

#     fastoutFilename = os.path.join(scriptDir, filename_IC)
#     df = FASTOutputFile(fastoutFilename).toDataFrame()
#     # print(df.keys())

#     ## Filtering requested conditions

#     """Filtering last 20 seconds for mean values"""
#     # TMax = 5539.9

#     # time
#     start_time = 0.0

#     # stop_time = len(df) # computation time increases by x20 !!!
#     stop_time = df["Time_[s]"].iloc[-1]
#     # print(stop_time)
#     step_time = 120.0
#     # set time to remove
#     remove_time = 100  # keeps last 20s
#     bin_time = np.arange(start_time, stop_time, step_time)
#     df_filtered = pd.DataFrame()

#     for i in bin_time:
#         df_temp = df[
#             (df["Time_[s]"] >= (i + remove_time)) & (df["Time_[s]"] < (i + step_time))
#         ]
#         # df_temp = df_temp.to_numpy()
#         df_filtered = pd.concat([df_filtered, df_temp])

#     # Filter inital condition for mean wind speeds in 2 m/s steps

#     WS_InitialCondition = windspeed
#     df_InitialConditions = pd.DataFrame()
#     for i in WS_InitialCondition:
#         df_temp_cond = df_filtered[
#             (df_filtered["Wind1VelX_[m/s]"] >= (i - 0.1))
#             & (df_filtered["Wind1VelX_[m/s]"] <= (i + 0.1))
#         ]
#         # df_temp = df_temp.to_numpy()
#         df_temp_cond_mean = df_temp_cond.mean(axis=0).round(4)
#         df_InitialConditions = pd.concat(
#             [df_InitialConditions, df_temp_cond_mean.to_frame().T], ignore_index=True
#         )

#     RPM = list(df_InitialConditions["RotSpeed_[rpm]"])
#     # Pitch angles for blade 1,2,3
#     PITCH_1 = list(df_InitialConditions["BldPitch1_[deg]"])
#     PITCH_2 = list(df_InitialConditions["BldPitch2_[deg]"])
#     PITCH_3 = list(df_InitialConditions["BldPitch3_[deg]"])
#     # OoPDefl     - Initial out-of-plane blade-tip displacement for blade 1,2,3
#     OoPDefl_1 = list(df_InitialConditions["TipDxc1_[m]"])
#     OoPDefl_2 = list(df_InitialConditions["TipDxc2_[m]"])
#     OoPDefl_3 = list(df_InitialConditions["TipDxc3_[m]"])
#     OoPDefl = (
#         np.array(OoPDefl_1) + np.array(OoPDefl_2) + np.array(OoPDefl_3)
#     ) / 3  # averaged
#     # IPDefl      - Initial in-plane blade-tip deflection (meters) for blade 1,2,3
#     IPDefl_1 = list(df_InitialConditions["TipDyc1_[m]"])
#     IPDefl_2 = list(df_InitialConditions["TipDyc2_[m]"])
#     IPDefl_3 = list(df_InitialConditions["TipDyc3_[m]"])
#     IPDefl = (np.array(IPDefl_1) + np.array(IPDefl_2) + np.array(IPDefl_3)) / 3
#     # TTDspFA     - Initial fore-aft tower-top displacement (meters)
#     TTDspFA = list(df_InitialConditions["TTDspFA_[m]"])
#     # TTDspSS     - Initial side-to-side tower-top displacement (meters)
#     TTDspSS = list(df_InitialConditions["TTDspSS_[m]"])
#     # Azimuth = list(df_InitialConditions['Azimuth_[deg]'])
#     return RPM, PITCH_1, PITCH_2, PITCH_3, OoPDefl, IPDefl, TTDspFA, TTDspSS


# ## Usage ecample
# # Set filename to analyze and specify wind speed steps
# # Start the timer
# start_time = time.time()

# ws_start = 3
# ws_stop = 25
# ws_step = 1
# windspeed = np.arange(ws_start, ws_stop + ws_step, ws_step)
# # filename_IC = '../results/FINAL/Basecase_clean/SteadyState.outb'
# filename_IC = r"D:\data\47_ruck\30_FAST\41_Senvion5M_final\Senvion5M_V1_V3-5-1_IC_fixed_Ptfm_DoFs\Senvion5M.out"

# RPM, PITCH_1, PITCH_2, PITCH_3, OoPDefl, IPDefl, TTDspFA, TTDspSS = Initial_Conditions(
#     filename_IC, windspeed
# )

# # Kombiniere die Daten zu einer Liste von Zeilen
# data = zip(windspeed, RPM, PITCH_1, OoPDefl, IPDefl, TTDspFA, TTDspSS)


# # Funktion zum Runden der Zahlenwerte auf 4 Nachkommastellen
# def round_values(values):
#     return [round(value, 4) if isinstance(value, float) else value for value in values]


# # Dateiname der CSV-Datei
# csv_filename = "output.csv"

# # Einheiten der Parameter
# units = ["(m/s)", "(rpm)", "(deg)", "(m)", "(m)", "(m)", "(m)"]

# # Schreibe die Daten in die CSV-Datei
# with open(csv_filename, mode="w", newline="") as file:
#     writer = csv.writer(file)
#     # Schreibe die Kopfzeile
#     writer.writerow(
#         [
#             "WindVxi",
#             "RotSpeed",
#             "BldPitch1",
#             "OoPDefl1",
#             "IPDefl1",
#             "TTDspFA",
#             "TTDspSS",
#         ]
#     )
#     # Schreibe die Einheitenzeile
#     writer.writerow(units)

#     # Schreibe die Datenzeilen
#     for row in data:
#         writer.writerow(round_values(row))

# print(f"Daten wurden in {csv_filename} gespeichert.")


# # # End the timer
# # end_time = time.time()
# # # Calculate and print the execution time
# # execution_time = end_time - start_time
# # print(f"Execution time: {execution_time} seconds")
