import simdriver
import matplotlib.pyplot as plt
import pandas as pd

SIM_TIME = 60

# Simulate steady wind conditions.
simdriver.run_fast(
    output_dir="data/output_steady",
    steady_wind_speed=[5, 15],
    steady_reference_height=90,
    steady_power_law_exponent=0.2,
    time_span=SIM_TIME,
    time_step=0.01,
    model_dir="extern/NREL_5MW",
    input_file="NREL_5MW.fst",
)

# Generate turbulent wind fields.
simdriver.run_turbsim(
    output_dir="data/wind",
    grid_points_horizontal=30,
    grid_points_vertical=40,
    grid_size_horizontal=150,
    grid_size_vertical=164,
    hub_height=90,
    wind_speed=[5, 15],
    turbulence_intensity=[10, 20],
    time_span=SIM_TIME,
    time_step=0.25,
    power_law_exponent=0.2,
    max_processes=2,
)

# Simulate turbulent wind conditions.
simdriver.run_fast(
    wind_files="data/wind",
    output_dir="data/output_turb",
    time_span=SIM_TIME,
    time_step=0.01,
    model_dir="extern/NREL_5MW",
    input_file="NREL_5MW.fst",
)

# Plot results.
steady = pd.read_parquet("data/output_steady/U_15_00.parquet")
turb = pd.read_parquet("data/output_turb/U_15_00_TI_10_00.parquet")

plt.figure()
plt.subplot(2, 1, 1)
plt.title("Wind Speed and Blade Pitch")
plt.plot(steady["time"], steady["v0"], label="steady wind")
plt.plot(turb["time"], turb["v0"], label="turbulent wind")
plt.ylabel("Wind Speed [m/s]")
plt.xlim(0, SIM_TIME)
plt.legend()

plt.subplot(2, 1, 2)
plt.plot(steady["time"], steady["pitch"], label="steady wind")
plt.plot(turb["time"], turb["pitch"], label="turbulent wind")
plt.xlabel("Time [s]")
plt.ylabel("Blade Pitch [deg]")
plt.xlim(0, SIM_TIME)
plt.legend()
plt.show()
