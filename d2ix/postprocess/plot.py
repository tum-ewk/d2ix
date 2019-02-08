import matplotlib.pyplot as plt
from pathlib import Path


def create_barplot(data, filters, title, attributes, other_bin_size,
                   other_name, synonyms, colors, tech_order):
    df = data.copy()
    for k, v in filters.items():
        df = df.loc[df[k].isin(v)].reset_index(drop=True)

    def _set_other(row):
        if row['technology'] in _tech.index:
            tech = row['technology']
        else:
            tech = other_name
        return tech

    _tech = df[['technology', 'lvl']].groupby(by='technology').sum()
    _tech['share'] = _tech['lvl'] / df['lvl'].sum()
    _tech = _tech[_tech['share'] > other_bin_size]
    df['technology'] = df.apply(_set_other, axis=1)

    columns = df.columns.tolist()
    columns.remove('node')
    df = df[columns]
    columns.remove('lvl')
    df = df.groupby(by=columns).sum().reset_index()

    _plot_df = df.pivot(index='year', values='lvl', columns='technology')

    # Create Plot Axes
    plt.style.use('ggplot')
    fig = plt.figure(figsize=(6, 3))
    ax = fig.add_subplot(111, facecolor='white')

    # Set Plot kwargs
    kwargs = {'kind': 'bar', 'lw': 0, 'ax': ax,
              'stacked': True, 'grid': True}

    if isinstance(tech_order, list):
        order = [i for i in _plot_df.columns if i not in tech_order]
        if other_name in order:
            order.remove(other_name)
            order = [other_name] + tech_order + order
            _plot_df = _plot_df[order]
    else:
        _data_order = _plot_df.columns.tolist()
        if other_name in _data_order:
            _data_order.remove(other_name)
            order = [other_name] + _data_order
            _plot_df = _plot_df[order]

    if colors:
        if other_name in _plot_df.columns:
            attributes['colors'][other_name] = '#96989b'
        kwargs['color'] = map(attributes['postprocess']['colors'].get,
                              _plot_df.columns)
    else:
        kwargs['colormap'] = 'Paired'
    if synonyms:
        _plot_df = _plot_df.rename(columns=attributes['synonyms'])
    _plot_df.plot(**kwargs)

    # Plot Legend
    handles, labels = ax.get_legend_handles_labels()
    legend = ax.legend(handles[::-1], labels[::-1], loc='center left',
                       prop={'size': 10}, bbox_to_anchor=(1.05, 0.5))
    legend.get_frame().set_facecolor('white')
    legend.get_frame().set_edgecolor('white')

    # Set title and axis
    ax.set_title(title, fontsize=10)
    ax.set_ylabel(df['unit'].unique()[0], fontsize=11)
    ax.set_xlabel('')

    # Rotate x-tick labels
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=40, ha='right')

    # Grid & Spines & Ticks
    ax.grid(axis=u'y', which=u'major', color='lightgray', linestyle='-',
            linewidth=0.5)
    ax.spines['left'].set_color('dimgray')
    ax.spines['bottom'].set_color('dimgray')
    ax.tick_params(axis=u'both', which=u'both',
                   length=0, width=0, color='white')

    # Save and show Plot
    Path('output').mkdir(exist_ok=True)
    plt.savefig(f'./output/{title}.pdf', bbox_inches='tight',
                facecolor=fig.get_facecolor(), edgecolor='none')
    plt.savefig(f'./output/{title}.png', bbox_inches='tight',
                facecolor=fig.get_facecolor(), edgecolor='none')
    fig.tight_layout()
    plt.show()
