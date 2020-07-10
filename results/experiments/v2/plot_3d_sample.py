#!/usr/bin/env python3

# This import registers the 3D projection, but is otherwise unused.
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401 unused import
from matplotlib import cbook
from matplotlib import cm
from matplotlib.colors import LightSource
import matplotlib.pyplot as plt
import numpy as np

# https://colorcet.pyviz.org/
import colorcet as cc # pip3 install colorcet

# color_map = None # uses fixed default color
# color_map = cc.m_linear_blue_5_95_c73_r #from https://colorcet.pyviz.org/
# color_map = cc.m_linear_blue_95_50_c20 #from https://colorcet.pyviz.org/
# color_map = cc.m_linear_green_5_95_c69 #from https://colorcet.pyviz.org/
# color_map = cc.m_linear_grey_10_95_c0 #from https://colorcet.pyviz.org/
color_map = cc.m_linear_ternary_blue_0_44_c57 #from https://colorcet.pyviz.org/
#color_map = cc.m_isoluminant_cgo_80_c38 #from https://colorcet.pyviz.org/

countries = ['Brasil', 'USA', 'France', 'Germany']
months = ['Apr', 'May', 'Jun', 'Jul']
temperatures = [ # from https://www.weatherbase.com/
    # Each column is a country, each row is a month
    [24.6, 11.1, 09.9, 07.0], # Apr: Brazil, USA, France, Germany
    [23.7, 16.0, 13.6, 11.7], # May: Brazil, USA, France, Germany
    [22.8, 20.4, 16.9, 15.0], # Jun: Brazil, USA, France, Germany
    [22.4, 22.9, 19.2, 16.6], # Jul: Brazil, USA, France, Germany
]

# For textual data we need a numeric representation
ticks_countries = range(0, len(countries))
ticks_months = range(0, len(months))

x, y = np.meshgrid( ticks_countries, ticks_months)
z = np.asarray( temperatures )

fig, ax = plt.subplots(subplot_kw=dict(projection='3d'))

plt.title('Average Temperature in Countries')

# Plotting the area
ax.plot_surface(x, y, z, rstride=1, cstride=1,
    cmap=color_map,
    antialiased=True,
    shade=True,
    alpha=0.95,
    edgecolor='black',
    linewidth=0.3,
    zorder=1,
)

# Plotting the text labels for each data point
for month,i_month in zip(months, ticks_months):
    for country,i_country in zip(countries, ticks_countries):
        temperature = temperatures[i_month][i_country]
        # Parameters are: position in X, position in Y, position in Z, text to plot
        ax.text(i_country, i_month, temperature, temperature,
            color='black',
            ha='left',
            va='baseline',
            bbox={
                'pad': 2,
                'alpha': 0.5,
                'facecolor': 'white',
                'linewidth': 0,
            },
        )

# Now we set the texts for the numeric representation of textual data
ax.set_xlabel('Countries')
ax.set_xticks(ticks_countries)
ax.set_xticklabels(countries)

ax.set_ylabel('Months')
ax.set_yticks(ticks_months)
ax.set_yticklabels(months)

ax.set_zlabel('Avg Temperature (C)')

# ax.set_zlim(bottom=0, top=30) # Uncomment this line to manually set the limits

fig.set_size_inches(8, 6)
for fmt in ('png','pdf'): #png, pdf, ps, eps, svg
    fig.savefig('plot_3d_sample.'+fmt,
        dpi=150,
        format=fmt,
        bbox_inches='tight'
    )

plt.close()
