# fib-index

Code repository for the paper, "Identifying and characterizing superspreaders\\of low-credibility content on Twitter."
- Authors: [Matthew R. DeVerna](https://www.matthewdeverna.com), [Rachith Aiyappa](https://rachithaiyappa.github.io/), [Diogo Pacheco](https://computerscience.exeter.ac.uk/staff/dp503?sm=dp503), John Bryden, and [Filippo Menczer](https://cnets.indiana.edu/fil/)
- [Arxiv paper](https://doi.org/10.48550/arXiv.2207.09524)

### Contents
- `data/`: data used in this project
    - We cannot share raw Decahose data as we would be breaking a contract with Twitter. Instead, we share tweet IDs for rehydration.
- `figures/`: publication figures
- `notebooks/`: Jupyter notebooks utilized to create the figures for the paper and conduct analyses
- `src_clean/`: scripts utilized for data collection and analysis
- `config.ini`: a project configuration file


### Code note
An early version of this paper referred to the $h$-index metric as the FIB-index.
As a result, the code reflects this nomenclature.