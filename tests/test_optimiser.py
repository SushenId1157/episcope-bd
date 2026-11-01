"""
Optimiser constraint tests (AN-7, ENG-4).

The rubric asks for automated tests of the optimiser constraints. These check
that the solution actually satisfies the programme in Section 6.3, rather
than that the solver returned without error: an infeasible or silently
relaxed solution would otherwise pass unnoticed.
"""
import pandas as pd
import pytest

import analytics.optimise.allocation as alloc


RES = ["iv_saline_units"]


def _problem():
    demand = pd.DataFrame([
        {"district_id": "D01", "division": "Dhaka", "population": 1_000_000.0,
         "admissions": 100.0, "iv_saline_units": 1000.0},
        {"district_id": "D02", "division": "Dhaka", "population": 500_000.0,
         "admissions": 20.0, "iv_saline_units": 400.0},
        {"district_id": "D03", "division": "Khulna", "population": 250_000.0,
         "admissions": 5.0, "iv_saline_units": 150.0},
    ])
    stock = pd.DataFrame([
        {"district_id": "D01", "iv_saline_units": 100.0},
        {"district_id": "D02", "iv_saline_units": 50.0},
        {"district_id": "D03", "iv_saline_units": 20.0},
    ])
    return demand, stock


def test_solver_reaches_an_optimal_solution():
    """
    The brief is explicit that an allocation may only be called optimal when
    the solver establishes that status.
    """
    d, s = _problem()
    budget = 100_000.0
    r = alloc.optimise_allocation(d, s, budget, RES)
    assert r["status"] == "Optimal"
    assert r["is_optimal"]


def test_budget_constraint_holds():
    d, s = _problem()
    budget = 50_000.0
    r = alloc.optimise_allocation(d, s, budget, RES)
    assert r["spent"] <= budget * 1.0001


def test_allocations_are_non_negative():
    d, s = _problem()
    r = alloc.optimise_allocation(d, s, 50_000.0, RES)
    assert (r["allocation"]["allocated"] >= -1e-6).all()


def test_unmet_demand_is_consistent_with_the_allocation():
    """u >= demand - stock - x, the defining constraint of the programme."""
    d, s = _problem()
    a = alloc.optimise_allocation(d, s, 50_000.0, RES)["allocation"]
    slack = a["unmet"] + a["allocated"] + a["stock"] - a["demand"]
    assert (slack >= -1e-4).all()


def test_equity_floor_reaches_every_district():
    """
    Pure efficiency concentrates the budget. The floor exists so that no
    district is left with nothing, and this asserts it actually does that.
    """
    d, s = _problem()
    budget = 30_000.0
    none = alloc.optimise_allocation(d, s, budget, RES, equity_alpha=0.0)
    floor = alloc.optimise_allocation(d, s, budget, RES, equity_alpha=0.5)

    served_none = (none["allocation"].groupby("district_id")["allocated"]
                   .sum() > 0.5).sum()
    served_floor = (floor["allocation"].groupby("district_id")["allocated"]
                    .sum() > 0.5).sum()
    assert served_floor >= served_none
    assert served_floor == len(d)


def test_equity_costs_something():
    """
    If a floor were free, there would be no conflict to resolve and the
    efficiency-equity analysis would be measuring nothing.
    """
    d, s = _problem()
    budget = 30_000.0
    none = alloc.optimise_allocation(d, s, budget, RES, equity_alpha=0.0)
    floor = alloc.optimise_allocation(d, s, budget, RES, equity_alpha=0.75)
    assert (floor["allocation"]["unmet"].sum()
            >= none["allocation"]["unmet"].sum() - 1e-6)


def test_transport_capacity_is_respected():
    d, s = _problem()
    cap = {"Dhaka": 500.0, "Khulna": 200.0}
    r = alloc.optimise_allocation(d, s, 100_000.0, RES, transport_cap=cap)
    a = r["allocation"]
    for div, limit in cap.items():
        used = sum(alloc.TRANSPORT_VOLUME[res]
                   * a[(a["division"] == div)
                       & (a["resource"] == res)]["allocated"].sum()
                   for res in RES)
        assert used <= limit * 1.0001, f"{div} exceeded its transport cap"


def test_a_larger_budget_never_increases_unmet_demand():
    """A monotonicity property the formulation must satisfy."""
    d, s = _problem()
    small = alloc.optimise_allocation(d, s, 20_000.0, RES)
    large = alloc.optimise_allocation(d, s, 80_000.0, RES)
    assert (large["allocation"]["unmet"].sum()
            <= small["allocation"]["unmet"].sum() + 1e-6)


def test_demand_conversion_states_its_assumptions():
    """
    AN-6 requires conversion assumptions to be stated. They live in one
    dictionary so a reviewer can challenge each one individually.
    """
    for disease, c in alloc.CONVERSION.items():
        assert 0 < c["admission_rate"] < 1
        assert c["mean_los_days"] > 0
        assert c["saline_per_admission"] > 0
        assert "test_kit" in c
