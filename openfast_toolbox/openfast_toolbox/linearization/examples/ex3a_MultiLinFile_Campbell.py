""" 
Script to post-process several linearization files generated by different OpenFAST simulations.

Each OpenFAST simulation is considered to be at a different operating point (OP).
Typically, this is run for several wind speed/RPM.

A Campbell diagram is plotted, showing the frequencies and damping of each modes for each operating point.

An attempt to identify the turbine modes is done by the script, but a manual sorting is usually needed.
This is done by opening the csv file generated (Campbell_ModesID.csv), and changing the indices. 

The "plot call" at the end of the script can then be repeated with the updated csv file.


"""
import os
import glob
import numpy as np
import matplotlib.pyplot as plt
import openfast_toolbox.linearization as lin

# Get current directory so this script can be called from any location
scriptDir = os.path.dirname(__file__)

# --- Script Parameters
fstFiles = glob.glob(os.path.join(scriptDir,'../../../data/linearization_outputs/*.fst')) # list of fst files where linearization were run, lin file will be looked for
# fstFiles = glob.glob(os.path.join(scriptDir,'../../../data/NREL5MW/5MW_Land_Lin_Rotating/*.fst')) # list of fst files where linearization were run, lin file will be looked for
fstFiles.sort() # Important for unittest

# --- Step 3: Run MBC, identify Modes, generate CSV files, and binary modes
# Find lin files, perform MBC, and try to identify modes. A csv file is written with the mode IDs.
OP, Freq, Damp, UnMapped, ModeData, modeID_file = lin.postproCampbell(fstFiles, writeModes=True, verbose=True)

# Edit the mode ID file manually to better identify/distribute the modes
print('[TODO] Edit this file manually: ',modeID_file)
# modeID_file='Campbell_ModesID_Sorted.csv'

# --- Step 4: Plot Campbell
fig, axes, figName =  lin.plotCampbellDataFile(modeID_file, 'ws', ylim=None)



# --- Step 5: Generate visualization data (for advanced users)

# --- Step 5a: Write viz files (Only useful if OpenFAST was run with WrVTK=3)
vizDict = {'VTKLinModes':2, 'VTKLinScale':10}  # Options for .viz file. Default values are: VTKLinModes=15, VTKLinScale=10, VTKLinTim=1, VTKLinTimes1=True, VTKLinPhase=0, VTKModes=None
vizFiles = lin.writeVizFiles(fstFiles, verbose=True, **vizDict)

# --- Step 5b: Run FAST with VIZ files to generate VTKs
import openfast_toolbox.case_generation.runner as runner
simDir = os.path.dirname(fstFiles[0])
fastExe = os.path.join(scriptDir, '../../../data/openfast.exe')
### Option 1 write a batch file and run it
# batchfile = runner.writeBatch(os.path.join(simDir,'_RUNViz.bat'), vizFiles, fastExe=fastExe, flags='-VTKLin')
# runner.runBatch(batchfile)
### Option 2: direct calls
# runner.run_cmds(vizFiles, fastExe, showOutputs=True, flags=['-VTKLin'])

# --- Step 5c: Convert VTKs to AVI - TODO
# %       Also, this is experimental and users might need to adapt the inputs and batchfile content
#     pvPython          = 'pvpython'; % path to paraview-python binary
#     pythonPlotScript  = 'C:/Work/FAST/matlab-toolbox/Campbell/plotModeShapes.py'; % path to python plot script
#     paraviewStateFile = 'C:/Work/FAST/matlab-toolbox/Campbell/ED_Surfaces.pvsm';  % path  to paraview State file
#     writeAVIbatch([simulationFolder '/_RunAVI.bat'], simulationFolder, operatingPointsFile, pvPython, pythonPlotScript, paraviewStateFile);

if __name__=='__main__':
#     plt.show()
    pass

if __name__=='__test__':
    # Something weird is happening on github action, order is different, 
    np.testing.assert_almost_equal(Freq['1st_Tower_FA'][:2], [0.324446, 0.331407],3)
    np.testing.assert_almost_equal(Damp['1st_Tower_FA'][:2], [0.00352, 0.06034],4)
    np.testing.assert_almost_equal(OP['WS_[m/s]'], [0, 3],2)
    np.testing.assert_almost_equal(OP['RotSpeed_[rpm]'], [0, 6.972],2)
