import os
import sys
import uuid
import gdown
import pandas as pd

def read_from_gdrive_link(public_link: str) -> str:
    """
    """
    # by default, download the text file to the same
    # directory that this Python script lives in.
    path_to_downloaded_file = f"{os.getcwd()}/glcloud_file_{uuid.uuid4()}.txt"

    # ensure that the gcloud link is formatted in the
    # way the the `gdown` package is expecting
    # (i.e., "https://drive.google.com/uc?id=<FILE ID HERE>")
    if "sharing" in public_link:
        # The below logic is dependent on the link being provided as described
        # in the challenge instructions.
        # I.e., https://drive.google.com/file/d/10WF0EwKH7pac1Pxp3BmRwC_1B1Lxuix0/view?usp=sharing
        gcloud_link = f"https://drive.google.com/uc?id={public_link.split('/')[-2]}"
    else:
        gcloud_link = public_link

    _ = gdown.download(gcloud_link, path_to_downloaded_file, quiet=True)
    return path_to_downloaded_file

def make_cycle_simple(claim_routes_list: list) -> list:
    """
    """
    simple_cycle = \
        claim_routes_list[0:1:] + \
        list(dict.fromkeys(claim_routes_list[1:-1:])) + \
        claim_routes_list[-1::]
    
    return simple_cycle

def parse_route_string(single_route: str) -> list:
    """
    """
    return single_route.rstrip().split("|")

def is_cycle(routes_list: list) -> bool:
    """
    Each element of `routes_list` is expected to be a list
    of strings each following the source data file format of
    '<source_system>|<destination_system>|<claim_id>|<status_code>'.
    
    NOTE THAT as currently written, this function will NOT deem
    a list of routes where an intermediary route (i.e., a route
    that is NOT at the beginning or end of the list) matches has a
    destination system that matches the source system of the first
    route. This is because we are working off of the assumption that
    a cycle must be one where the FINAL destination system matches
    the first source sytstem.
    
    Another thing to note is that this function WILL deem self routes
    (i.e., claim only has one route and the source and destination
    systems match exactly) a cycle.
    """
    # first step is to explicitly validate that the same
    # `claim_id` and `status_code` values are shared across
    # entry element of the list.
    unique_statuses = set([parse_route_string(route)[-1] for route in routes_list])
    unique_claims = set([parse_route_string(route)[-2] for route in routes_list])
    consistent_ids_result = all([len(unique_statuses) == 1, len(unique_claims) == 1,])
    
    # next validate that the source system of the first route
    # matches the destination system of the last route.
    original_sys = parse_route_string(routes_list[0])[0]
    final_sys = parse_route_string(routes_list[-1])[1]
    sys_match_result = original_sys == final_sys

    # because of the contraint that a chain of claims has to be "simple"
    # to be deemed a Cycle, we must check that "simplifying" (i.e., removing
    # any repeated nodes) a cycle does NOT result in a different/shorter
    # chain of nodes.
    simplified_chain = make_cycle_simple(claim_routes_list=routes_list)
    simple_check = len(simplified_chain) == len(routes_list)
    
    # as a final check, validate that the destination sys. of a given
    # claim MATCHES the sources sys. of the claim that immediately
    # follows it. This to gurantee that a "passed" cycle follows
    # directional, continuous edges.
    match_results = []
    for i, claim in enumerate(routes_list):
        if i == 0:
            continue
        else:
            previous_destination = parse_route_string(routes_list[i - 1])[1]
            current_source = parse_route_string(claim)[0]
            
            _ = match_results.append(previous_destination == current_source)
        
    directional_edges_result = all(match_results)
    
    # ensure all above checks passed in order to identify a given
    # list of claims is a cycle.
    return all([consistent_ids_result, sys_match_result, simple_check, directional_edges_result])

def main(path_to_txt_file: str, encoding="UTF-8") -> str:
    """
    """
    opened_file = open(path_to_txt_file, mode="r", encoding=encoding)
    
    list_of_cycles, claim_routes_list = [], []
    new_claim_identified = True
    for line_number, line in enumerate(opened_file):
        line_list = parse_route_string(single_route=line)
        new_claim_id = line_list[-2]
        
        if new_claim_identified:
            # i.e., we are reading the first route of the new 'claim_id'
            # we have encountered.
            claim_id_to_match = new_claim_id
            original_status_code = line_list[-1]
            #if line not in claim_routes_list:
            _ = claim_routes_list.append(line)
            new_claim_identified = False
        else:
            # i.e., we are still incrementally reading the txt file to
            # keep trying to find more routes that have the same claim_id
            # we are currently working with.
            if new_claim_id == claim_id_to_match:
                # i.e., we have identified another route that corresponds to
                # the same claim_id that we are currently working with.
                _ = claim_routes_list.append(line)
                new_claim_identified = False
            
            else:
                # we NO LONGER reading a line of text that corresponds to
                # the same claim_id that we are currently working with.
                cycle_result = is_cycle(routes_list=claim_routes_list)
                if cycle_result:
                    _ = list_of_cycles.append("{},{},{}".format(
                        claim_id_to_match,
                        original_status_code,
                        len(claim_routes_list)
                    ))
                
                new_claim_identified = True
                claim_routes_list = [line]
    
    # upon reading the last line of the .txt file, the inner-most
    # else statement will NOT return `True`. For that reason, we
    # need to include another line that uses the `is_cycle` utility
    # function to ensure this last claim chain is also considered.
    cycle_result = is_cycle(routes_list=claim_routes_list)
    if cycle_result:
        _ = list_of_cycles.append("{},{},{}".format(
            line_list[-2],
            line_list[-1],
            len(claim_routes_list)
        ))
    
    opened_file.close()
    
    # next, we have to determine the longest cycle. Return to the user the
    # summary report of this longest cycle.
    df = pd.DataFrame(
        [cycle.split(",") for cycle in list_of_cycles],
        columns=["claim_id", "status_code", "cycle_length"]
    )
    return list_of_cycles[df["cycle_length"].astype(float).idxmax()]


if __name__ == "__main__":
    specified_file = sys.argv[1]
    file_path = \
        specified_file if (specified_file.endswith(".txt") and "http" not in specified_file) \
        else read_from_gdrive_link(public_link=specified_file)

    longest_cycle_summary = main(path_to_txt_file=file_path)
    _ = sys.stdout.write(longest_cycle_summary)