
import pandas as pd
import numpy as np 
import random
import datetime


class assign():
    def __init__(self, mode, subnet, iter, i, incremental_update_shadow_price, tie_break_rule, decimal):
        self.decimal = decimal
        self.subnet = subnet
        self.iter = iter  # total number of iters
        self.i  = i # i th iter
        self.mode = mode
        self.tie_break_rule = tie_break_rule
        self.tau = self.subnet.exp_time_hoirzon*(self.i/self.iter) + self.subnet.exp_start
        self.incremental_update_shadow_price = incremental_update_shadow_price
        self.initialize_accumulative_assignment()
        self.assignment_decisions(self.mode)

        if self.i == self.iter - 1: # store assignment if last iteration
            self.update_shipment_data_according_to_assignemnt()

    def initialize_accumulative_assignment(self):
        for j in range(len(self.subnet.J)):
            self.subnet.J[j].assignment_by_resolve[self.i] = 0


    def create_route_cost_map(self, o, mode):
        self.subnet.O[o].route_cost_map = {}
        for route in self.subnet.O[o].feasible_routes:
            r = self.subnet.R_nametoindex[route]
            resource_list = self.subnet.R[r].resources
            self.subnet.R[r].route_cost = self.subnet.R[r].base_cost
            if mode == 'DPS':
                for resource in resource_list:
                    j = self.subnet.J_nametoindex[resource]
                    if self.incremental_update_shadow_price == True:
                        self.subnet.R[r].route_cost += self.subnet.J[j].shadow_price + self.incremental_update(o,j, self.tau)
                    else:
                        self.subnet.R[r].route_cost += self.subnet.J[j].shadow_price
                self.subnet.O[o].route_cost_map[route] = round(self.subnet.R[r].route_cost,self.decimal)
            elif mode == 'greedy':
                self.subnet.O[o].route_cost_map[route] = round(self.subnet.R[r].route_cost,self.decimal)
            elif mode == 'no_cost':
                self.subnet.O[o].route_cost_map[route] = 0
            else:
                print('no valid mode')

    def iterate_through_routes(self, o):
        deleted_route = []
        chosen_route_index = 'no_feasible_route'
        dic_resource_cap = 'no_feasible_route'
        while len(deleted_route) < len(self.subnet.O[o].feasible_routes):
            temp_chosen_route_index = self.get_min_cost_routes(o, deleted_route, self.tie_break_rule)
            resource_list = self.get_reousrce_list(temp_chosen_route_index)
            if resource_list.tolist() == []:
                chosen_route_index = temp_chosen_route_index
                dic_resource_cap = {}
                break
            else:                
                min_remaining_cap, temp_dic_resource_cap = self.min_cap_of_resources(resource_list)
                deleted_route.append(temp_chosen_route_index)
                if min_remaining_cap >=1:
                    chosen_route_index = temp_chosen_route_index
                    dic_resource_cap = temp_dic_resource_cap
                    break
        self.subnet.O[o].assigned_route = chosen_route_index
        self.subnet.O[o].resource_caps_of_assigned_route = dic_resource_cap
        return chosen_route_index
    
    def get_min_cost_routes(self, o, deleted_route, tie_break_rule):
        cleaned_map = self.subnet.O[o].route_cost_map.copy()
        if deleted_route != []:
            for route in deleted_route:
                cleaned_map.pop(route)
        min_route_cost = min(cleaned_map.values())
        min_cost_routes_lst = [k for k, v in cleaned_map.items() if v == min_route_cost]

        def pick_by_route_category(route_index_lst):
            lst_3p_route = []
            lst_ddu_route = []
            lst_amzl_direct_route = []
            lst_amzl_indir_route = []
            for i in range(len(route_index_lst)):
                route = route_index_lst[i]
                if self.subnet.R[self.subnet.R_nametoindex[route]].type == '3p':
                    lst_3p_route.append(route)
                elif self.subnet.R[self.subnet.R_nametoindex[route]].type == 'ddu':
                    lst_ddu_route.append(route)
                elif self.subnet.R[self.subnet.R_nametoindex[route]].type == 'amzl_direct':
                    lst_amzl_direct_route.append(route)
                elif self.subnet.R[self.subnet.R_nametoindex[route]].type == 'amzl_indirect':
                    lst_amzl_indir_route.append(route)
                else:
                    print('error, len of min_cost_cpt_route_lst', len(route_index_lst))
            if len(lst_amzl_direct_route) != 0:
                return random.choice(lst_amzl_direct_route)
            elif len(lst_amzl_indir_route) != 0:
                return random.choice(lst_amzl_indir_route)
            elif len(lst_ddu_route) != 0:
                return random.choice(lst_ddu_route)
            elif len(lst_3p_route) != 0:
                return random.choice(lst_3p_route)

        def route_with_earliest_cpt(min_cost_routes_lst):
            min_cost_route_firstcpt_map = {}
            for route in min_cost_routes_lst:
                min_cost_route_firstcpt_map[route] = self.subnet.R[self.subnet.R_nametoindex[route]].first_cpt
            min_cpt = min(min_cost_route_firstcpt_map.values())
            min_cost_cpt_routes_lst = [k for k, v in min_cost_route_firstcpt_map.items() if v == min_cpt]
            return min_cpt, min_cost_cpt_routes_lst
        
        def route_with_dayone_cpt(min_cost_routes_lst):
            min_cost_route_firstcpt_map = {}
            for route in min_cost_routes_lst:
                min_cost_route_firstcpt_map[route] = self.subnet.R[self.subnet.R_nametoindex[route]].first_cpt
            min_cost_firstday_route_lst = [k for k, v in min_cost_route_firstcpt_map.items() if v < datetime.datetime(2021, 5, 12,0,0,0)]
            return min_cost_firstday_route_lst

        def choose_route_from_map(min_cost_routes_lst, tie_break_rule):
            min_cpt, min_cost_cpt_routes_lst = route_with_earliest_cpt(min_cost_routes_lst)
            
            if tie_break_rule == 'by_cpt':
                chosen_route = random.choice(min_cost_cpt_routes_lst) 
            elif tie_break_rule == 'by_cpt_daylevel_then_routecat':
                if min_cpt >= datetime.datetime(2021, 5, 12,0,0,0):
                    chosen_route = pick_by_route_category(min_cost_routes_lst)
                else:
                    min_cost_firstday_route_lst = route_with_dayone_cpt(min_cost_routes_lst)
                    chosen_route = pick_by_route_category(min_cost_firstday_route_lst)
            elif tie_break_rule == 'by_cpt_hourlevel_then_routecat':
                chosen_route = pick_by_route_category(min_cost_cpt_routes_lst)
            elif tie_break_rule == 'complete_random':
                chosen_route = random.choice(min_cost_routes_lst)
            else:
                print('no valid tie breaking rule')

            return chosen_route

        chosen_route = choose_route_from_map(min_cost_routes_lst, tie_break_rule)
        return chosen_route

    def get_reousrce_list(self, route_index):
        return self.subnet.route_data[self.subnet.route_data['index'] == route_index].resources.item()
                
    def min_cap_of_resources(self, resource_list):
        relevent_resource = self.subnet.resource_data[self.subnet.resource_data['index'].isin(resource_list)]
        min_cap = relevent_resource.remaining_cap.min()
        dic_cap =  dict(zip(relevent_resource['index'], relevent_resource['remaining_cap']))
        return min_cap, dic_cap

    def update_capacity_by_assignmnet(self, chosen_route_index, o):
        resource_list = self.get_reousrce_list(chosen_route_index)
        for resource in resource_list:
            self.subnet.resource_data.loc[self.subnet.resource_data['index'] == resource, 'remaining_cap'] -= 1
            self.subnet.J[self.subnet.J_nametoindex[resource]].remaining_cap -= 1
            self.subnet.J[self.subnet.J_nametoindex[resource]].assignment_by_resolve[self.i] += 1
            self.subnet.J[self.subnet.J_nametoindex[resource]].lst_assigned_time_stamp.append(self.subnet.O[o].arrival)
    
    def create_order_list(self):
        start_time = self.tau
        end_time = self.subnet.exp_time_hoirzon*((self.i+1)/self.iter) + self.subnet.exp_start # check add round up 
        if self.i != self.iter - 1:
            order_list = list(self.subnet.shipment_data[(self.subnet.shipment_data['arrival']>= start_time)&(self.subnet.shipment_data['arrival']< end_time)].index)
        else:
            order_list = list(self.subnet.shipment_data[(self.subnet.shipment_data['arrival']>= start_time)].index)
        return order_list

    def incremental_update(self, o, j, tau):
        arrival_time = self.subnet.O[o].arrival

        def max_cpt_of_upstream(j):
            lst_cpt = []
            for r in self.subnet.R_j[j]:
                lst_cpt.append(self.subnet.R[r].first_cpt)
            return max(lst_cpt)

        if (self.subnet.J[j].type) in ['3p', 'fcds_resource', 'fcsc_resource']:
            t_resource_end = self.subnet.J[j].cpt
        elif self.subnet.J[j].type in ['ddu', 'sc_resource','ds_resource', 'scds_resource']:
            t_resource_end = max_cpt_of_upstream(j)
        else:
            print('error in incremental_update')


        fraction = (arrival_time - tau)/(min(self.subnet.exp_end, t_resource_end) - tau)
        planned_flow = self.subnet.J[j].optimal_flow
        accumulative_assignment = self.subnet.J[j].assignment_by_resolve[self.i]
        adjusted_flow = accumulative_assignment - fraction *planned_flow
        incremental_update = (self.subnet.J[j].penalty_cost)*adjusted_flow
        return incremental_update

    def update_shipment_data_according_to_assignemnt(self):
        self.subnet.shipment_data['assigned_route'] = np.array([self.subnet.O[o].assigned_route for o in range(len(self.subnet.O))])
        self.subnet.shipment_data['resource_caps_of_assigned_route'] = np.array([self.subnet.O[o].resource_caps_of_assigned_route for o in range(len(self.subnet.O))])
        self.subnet.shipment_data['route_cost_map'] = np.array([self.subnet.O[o].route_cost_map for o in range(len(self.subnet.O))])
        self.subnet.resource_data['lst_assigned_time_stamp'] = pd.Series([self.subnet.J[j].lst_assigned_time_stamp for j in range(len(self.subnet.J))])
        self.subnet.resource_data['lst_shadow_price'] = pd.Series([self.subnet.J[j].lst_shadow_price for j in range(len(self.subnet.J))])

    def assignment_decisions(self, mode):
        
        if mode != 'DPS':
            self.subnet.resource_data['remaining_cap'] = np.array([self.subnet.J[j].remaining_cap for j in range(len(self.subnet.J))])

        self.order_list = self.create_order_list()
        
        for o in self.order_list:
            # break if no feasible route for an order
            if len(self.subnet.O[o].feasible_routes) == 0:
                self.subnet.O[o].assigned_route = 'no_feasible_route'
                break
            
            # create route_cost_map
            self.create_route_cost_map(o, mode)
            chosen_route_index = self.iterate_through_routes(o)

            if chosen_route_index != 'no_feasible_route':
                self.update_capacity_by_assignmnet(chosen_route_index, o)


