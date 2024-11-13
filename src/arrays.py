def single_year_single_var_all_levels(
    years: list, 
    single_variables: list=[], 
    multi_variables:list=[],
    levels: list=None,
    index: int=None
):
    """
    Configure with:
     - 1 year per job
     - single variable / level
    """
    years = [[y] for y in years]
    
    variables = single_variables + multi_variables
    
    num_jobs = len(years) * len(variables)
    
    if index is None:
        return num_jobs
    
    # identify the year / variable pair for the given index
    yindex = index % len(years)
    vindex = index // len(years)    
    
    years = years[yindex]
    variables = variables[vindex]
    
    if variables in single_variables:
        levels = None
    else:
        levels = levels
        
    return years, variables, levels
    
def n_years_single_var_all_levels(
    years: list, 
    n_years: int,
    single_variables: list=[], 
    multi_variables:list=[],
    levels: list=None,
    index: int=None
):
    """
    Configure with:
     - n_years years per job
     - single variable / level
    """
    years = [years[i:i+n_years] for i in range(0, len(years), n_years)]
    
    
    variables = single_variables + multi_variables
    
    num_jobs = len(years) * len(variables)
    
    if index is None:
        return num_jobs
    
    # identify the year / variable pair for the given index
    yindex = index % len(years)
    vindex = index // len(years)    
    
    years = years[yindex]
    variables = variables[vindex]
    
    if variables in single_variables:
        levels = None
    else:
        levels = levels
        
    return years, variables, levels
    
if __name__ == "__main__":
    import yaml
    import argparse
    
    p = argparse.ArgumentParser()
    p.add_argument("--years", type=str, nargs="+",)
    p.add_argument("--single_variables", type=str, nargs="+",required=False)
    p.add_argument("--multi_variables", type=str, nargs="+",required=False)
    p.add_argument("--levels", type=str, nargs="+",required=False)
    p.add_argument("--index", type=int, default=None)
    p.add_argument("--strategy", type=str, choices=["single_year_single_var_all_levels", "n_years_single_var_all_levels"], required=True)
    p.add_argument("--n_years", type=int, default=None)
    args = p.parse_args()
    
    kwargs = vars(args)
    
    strategy = kwargs.pop("strategy")
    
    if strategy == "single_year_single_var_all_levels":
        out = single_year_single_var_all_levels(**kwargs)
        
    elif strategy == "n_years_single_var_all_levels":
        assert args.n_years is not None, "n_years must be specified."
        out = n_years_single_var_all_levels(**kwargs)
        
    else:
        raise ValueError("Invalid arguments. No strategy found.")
    
    years, variables, levels = out
    ystring = f"{' '.join(str(y) for y in years)}"
    vstring = variables
    lstring = f"{' '.join(str(l) for l in levels)}" if levels is not None else levels
    
    print(f"{ystring},{vstring},{lstring}")
    