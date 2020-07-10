def plot_3d(applications, experiments, averages, title, xlabel, ylabel, zlabel, filename):
    # This import registers the 3D projection, but is otherwise unused.
    from mpl_toolkits.mplot3d import Axes3D  # noqa: F401 unused import
    from matplotlib import cbook
    from matplotlib import cm
    from matplotlib.colors import LightSource
    import matplotlib.pyplot as plt
    import numpy as np

    # https://colorcet.pyviz.org/
    import colorcet as cc # pip3 install colorcet

    color_map = cc.m_linear_ternary_blue_0_44_c57 #from https://colorcet.pyviz.org/

    #applications = ['Brasil', 'USA', 'France', 'Germany']
    #experiments = ['Apr', 'May', 'Jun', 'Jul']
    #averages = [ # from https://www.weatherbase.com/
    #    # Each column is a country, each row is a month
    #    [24.6, 11.1, 09.9, 07.0], # Apr: Brazil, USA, France, Germany
    #    [23.7, 16.0, 13.6, 11.7], # May: Brazil, USA, France, Germany
    #    [22.8, 20.4, 16.9, 15.0], # Jun: Brazil, USA, France, Germany
    #    [22.4, 22.9, 19.2, 16.6], # Jul: Brazil, USA, France, Germany
    #]

    # For textual data we need a numeric representation
    ticks_applications = range(0, len(applications))
    ticks_experiments = range(0, len(experiments))

    x, y = np.meshgrid( ticks_applications, ticks_experiments)
    z = np.asarray( averages )

    fig, ax = plt.subplots(subplot_kw=dict(projection='3d'))

    plt.title(title)

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
    for experiment,i_experiment in zip(experiments, ticks_experiments):
        for application,i_application in zip(applications, ticks_applications):
            average = averages[i_experiment][i_application]
            # Parameters are: position in X, position in Y, position in Z, text to plot
            ax.text(i_application, i_experiment, average, average,
                color='black',
                fontsize=8,
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
    ax.set_xlabel(xlabel)
    ax.set_xticks(ticks_applications)
    ax.set_xticklabels(applications, fontsize=8)

    ax.set_ylabel(ylabel, x=100)
    ax.set_yticks(ticks_experiments)
    ax.set_yticklabels(experiments, fontsize=8)

    ax.set_zlabel(zlabel)

    # ax.set_zlim(bottom=0, top=30) # Uncomment this line to manually set the limits

    fig.set_size_inches(8, 6)
    for fmt in ('png','pdf'): #png, pdf, ps, eps, svg
        fig.savefig(filename + '.' + fmt,
            dpi=150,
            format=fmt,
            bbox_inches='tight'
        )

    plt.close()
