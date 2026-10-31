"""
EpiScope - resource demand conversion and allocation optimisation
(AN-6, AN-7, AN-8).

Two stages:

1. Demand conversion. Forecast case counts become bed-days, saline units and
   test kits through stated clinical ratios. Every ratio below is a modelling
   assumption of this teaching project, chosen to be plausible and documented,
   not a figure taken from any ministry standard. They are collected in one
   place so a reviewer can challenge them individually and so sensitivity to
   them can be tested.

2. Allocation. A linear programme distributes a fixed national budget across
   districts to minimise weighted unmet demand, subject to an equity floor and
   per-division transport capacity, following the formulation in Section 6.3.

Why an upper quantile of the forecast drives demand rather than the mean.
Planning to the mean leaves a district short in half of all weeks, and the
cost of being short is not symmetric with the cost of holding surplus stock:
one is a stockout during an outbreak, the other is inventory. The 80%
prediction interval's upper bound is used, which AN-5 showed to be calibrated
(empirical coverage 0.846 against a nominal 0.80).
"""
from __future__ import annotations

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Clinical conversion assumptions (AN-6)
# ---------------------------------------------------------------------------
# admission_rate      share of reported cases admitted to hospital
# mean_los_days       mean length of stay for an admitted case
# saline_per_admission  litres of IV fluid per admitted case over the stay
# saline_per_outpatient litres issued to non-admitted cases (ORS for most)
# tests_per_case      diagnostic tests used per reported case, including
#                     negatives, which is why it exceeds 1
#
# Sources of the ranges: the admission rates are those the generator itself
# used, so demand conversion is consistent with the data-generating process;
# lengths of stay and consumable rates are plausible values for the clinical
# course of each disease. The report states these as assumptions and the
# efficiency-equity analysis shows how conclusions move if they are wrong.
CONVERSION = {
    "dengue": {
        "admission_rate": 0.17,
        "mean_los_days": 4.2,
        "saline_per_admission": 9.0,
        "saline_per_outpatient": 0.5,
        "tests_per_case": 1.6,          # NS1 plus confirmatory
        "test_kit": "ns1_kits",
    },
    "diarrhoeal": {
        "admission_rate": 0.11,
        "mean_los_days": 2.6,
        "saline_per_admission": 6.5,
        "saline_per_outpatient": 1.2,   # ORS issued widely
        "tests_per_case": 0.35,         # most treated syndromically
        "test_kit": "ors_sachets",
    },
    "ili": {
        "admission_rate": 0.05,
        "mean_los_days": 3.1,
        "saline_per_admission": 4.0,
        "saline_per_outpatient": 0.1,
        "tests_per_case": 0.20,
        "test_kit": "ns1_kits",
    },
    "malaria": {
        "admission_rate": 0.14,
        "mean_los_days": 3.4,
        "saline_per_admission": 5.0,
        "saline_per_outpatient": 0.2,
        "tests_per_case": 1.3,          # RDT used widely
        "test_kit": "antimalarial_doses",
    },
}

# Unit costs in BDT. Placeholder values for this teaching case, documented as
# such; the optimiser's structure does not depend on their exact level, and
# the efficiency-equity curve is reported across budget levels precisely so
# the recommendation does not hinge on one cost assumption.
UNIT_COST = {
    "beds": 0.0,              # capacity, not purchased within the horizon
    "iv_saline_units": 95.0,
    "ns1_kits": 180.0,
    "ors_sachets": 12.0,
    "antimalarial_doses": 140.0,
}

# Transport capacity per division, in volume units per planning horizon.
# Bulk fluids dominate volume, so saline is weighted far above test kits.
TRANSPORT_VOLUME = {
    "iv_saline_units": 1.0,
    "ns1_kits": 0.05,
    "ors_sachets": 0.02,
    "antimalarial_doses": 0.04,
}


def convert_to_demand(fc: pd.DataFrame, horizon_weeks: int = 4
                      ) -> pd.DataFrame:
    """
    Turn forecast cases into resource requirements per district.

    `fc` must carry district_id, district_name, division, disease_group and
    the forecast columns. The upper 80% bound drives demand, with the point
    forecast kept alongside so the report can show what planning to the mean
    would have given.
    """
    rows = []
    for _, r in fc.iterrows():
        c = CONVERSION.get(r["disease_group"])
        if c is None:
            continue
        planning_cases = float(r["upper_80"])
        expected_cases = float(r["forecast"])

        for label, cases in (("planning", planning_cases),
                             ("expected", expected_cases)):
            adm = cases * c["admission_rate"]
            bed_days = adm * c["mean_los_days"]
            saline = (adm * c["saline_per_admission"]
                      + (cases - adm) * c["saline_per_outpatient"])
            tests = cases * c["tests_per_case"]
            rows.append({
                "district_id": r["district_id"],
                "district_name": r["district_name"],
                "division": r["division"],
                "disease_group": r["disease_group"],
                "basis": label,
                "cases": cases,
                "admissions": adm,
                "bed_days": bed_days,
                "beds_needed": bed_days / 7.0,     # concurrent beds per week
                "iv_saline_units": saline,
                c["test_kit"]: tests,
            })
    out = pd.DataFrame(rows).fillna(0.0)
    num = [c for c in out.columns if c not in
           ("district_id", "district_name", "division", "disease_group",
            "basis")]
    return (out.groupby(["district_id", "district_name", "division", "basis"],
                        as_index=False)[num].sum())


def optimise_allocation(demand: pd.DataFrame, stock: pd.DataFrame,
                        budget: float, resources: list[str],
                        severity_weight: dict[str, float] | None = None,
                        equity_alpha: float = 0.0,
                        transport_cap: dict[str, float] | None = None
                        ) -> dict:
    """
    Solve the allocation linear programme of Section 6.3.

        min  sum_d sum_r w_d * u_{d,r}
        s.t. u_{d,r} >= yhat_{d,r} - s_{d,r} - x_{d,r}
             sum_d sum_r c_r x_{d,r} <= B
             x_{d,r} >= alpha * p_d * sum_d' x_{d',r}
             sum_{d in div k} sum_r v_r x_{d,r} <= T_k

    Returns the solved allocation plus the solver status. An allocation is
    only described as optimal when the solver actually reports an optimal
    status; anything else is reported as what it is.
    """
    import pulp

    districts = demand["district_id"].tolist()
    pop = dict(zip(demand["district_id"], demand.get(
        "population", pd.Series([1.0] * len(demand)))))
    total_pop = sum(pop.values()) or 1.0
    pop_share = {d: pop[d] / total_pop for d in districts}
    div_of = dict(zip(demand["district_id"], demand["division"]))
    w = severity_weight or {d: 1.0 for d in districts}

    dem = {(r["district_id"], res): float(r.get(res, 0.0))
           for _, r in demand.iterrows() for res in resources}
    stk = {(r["district_id"], res): float(r.get(res, 0.0))
           for _, r in stock.iterrows() for res in resources}

    prob = pulp.LpProblem("episcope_allocation", pulp.LpMinimize)
    x = {(d, r): pulp.LpVariable(f"x_{d}_{r}", lowBound=0)
         for d in districts for r in resources}
    u = {(d, r): pulp.LpVariable(f"u_{d}_{r}", lowBound=0)
         for d in districts for r in resources}

    prob += pulp.lpSum(w.get(d, 1.0) * u[(d, r)]
                       for d in districts for r in resources)

    for d in districts:
        for r in resources:
            need = dem.get((d, r), 0.0) - stk.get((d, r), 0.0)
            prob += u[(d, r)] >= need - x[(d, r)], f"unmet_{d}_{r}"

    prob += pulp.lpSum(UNIT_COST.get(r, 0.0) * x[(d, r)]
                       for d in districts for r in resources) <= budget, "budget"

    # Equity floor: every district receives at least alpha times its
    # population share of what is distributed. alpha = 0 is pure efficiency;
    # raising it guarantees low-risk districts a minimum, at the cost of
    # leaving more unmet demand where the need is greatest. This is the
    # policy dial of conflict C3.
    if equity_alpha > 0:
        for r in resources:
            tot = pulp.lpSum(x[(d2, r)] for d2 in districts)
            for d in districts:
                prob += (x[(d, r)] >= equity_alpha * pop_share[d] * tot,
                         f"equity_{d}_{r}")

    if transport_cap:
        divisions = sorted(set(div_of.values()))
        for k in divisions:
            cap = transport_cap.get(k)
            if cap is None:
                continue
            prob += (pulp.lpSum(TRANSPORT_VOLUME.get(r, 1.0) * x[(d, r)]
                                for d in districts if div_of[d] == k
                                for r in resources) <= cap,
                     f"transport_{k}")

    # Section 6.3 permits PuLP/CBC, OR-Tools or SciPy HiGHS. CBC ships with
    # PuLP 2.x and is used here; the solver that actually ran is recorded
    # rather than assumed, because a clean clone on another machine may
    # resolve a different default and the report has to state which was used.
    solver = None
    for name in ("PULP_CBC_CMD", "COIN_CMD", "HiGHS_CMD", "HiGHS"):
        cls = getattr(pulp, name, None)
        if cls is None:
            continue
        try:
            cand = cls(msg=0)
            if cand.available():
                solver = cand
                break
        except Exception:
            continue

    if solver is not None:
        prob.solve(solver)
        solver_name = type(solver).__name__
    else:
        prob.solve()
        solver_name = (type(pulp.LpSolverDefault).__name__
                       if pulp.LpSolverDefault else "PuLP default")
    status = pulp.LpStatus[prob.status]

    alloc = [{"district_id": d, "division": div_of[d], "resource": r,
              "demand": dem.get((d, r), 0.0),
              "stock": stk.get((d, r), 0.0),
              "allocated": float(x[(d, r)].value() or 0.0),
              "unmet": float(u[(d, r)].value() or 0.0)}
             for d in districts for r in resources]

    spent = sum(UNIT_COST.get(a["resource"], 0.0) * a["allocated"]
                for a in alloc)
    return {"status": status,
            "solver": solver_name,
            "is_optimal": status == "Optimal",
            "objective": float(pulp.value(prob.objective) or 0.0),
            "budget": budget, "spent": spent,
            "budget_utilisation": spent / budget if budget else 0.0,
            "equity_alpha": equity_alpha,
            "allocation": pd.DataFrame(alloc)}


def population_proportional_baseline(demand: pd.DataFrame, stock: pd.DataFrame,
                                     budget: float, resources: list[str]
                                     ) -> dict:
    """
    The comparator AN-7 requires: split the budget by population share.

    This is what a ministry does without an optimiser, and it is a fair
    comparison rather than a straw man, since population is a reasonable
    proxy for need in the absence of forecasts.
    """
    pop = demand.get("population", pd.Series([1.0] * len(demand)))
    total_pop = float(pop.sum()) or 1.0
    rows = []
    for _, r in demand.iterrows():
        share = float(r.get("population", 1.0)) / total_pop
        d_budget = budget * share
        # Spend a district's share across resources in proportion to its need
        needs = {res: max(0.0, float(r.get(res, 0.0))) for res in resources}
        cost_of_need = sum(UNIT_COST.get(res, 0.0) * n
                           for res, n in needs.items()) or 1.0
        scale = min(1.0, d_budget / cost_of_need)
        for res in resources:
            got = needs[res] * scale
            rows.append({"district_id": r["district_id"],
                         "division": r["division"], "resource": res,
                         "demand": float(r.get(res, 0.0)),
                         "stock": 0.0, "allocated": got,
                         "unmet": max(0.0, float(r.get(res, 0.0)) - got)})
    df = pd.DataFrame(rows)
    spent = sum(UNIT_COST.get(a, 0.0) * g
                for a, g in zip(df["resource"], df["allocated"]))
    return {"status": "PopulationProportional", "is_optimal": False,
            "objective": float(df["unmet"].sum()), "budget": budget,
            "spent": spent, "budget_utilisation": spent / budget if budget else 0,
            "allocation": df}


# ---------------------------------------------------------------------------
# Scenario construction (AN-7)
# ---------------------------------------------------------------------------
# The first optimiser run returned zero unmet demand while spending nothing,
# on a budget covering only 60% of need. That is arithmetically impossible,
# and the diagnosis was a modelling error rather than a solver one: S6 stock
# is generated from bed counts and never consulted the case forecast, so every
# district already held 4x to 60x its four-week requirement.
#
# Measured stock-to-demand ratios over the planning horizon:
#     iv_saline_units      4.2x
#     ns1_kits             4.4x
#     ors_sachets         60.0x
#     antimalarial_doses   no demand at all
#
# A single usable fraction cannot fix this, because the ratios differ by an
# order of magnitude. Each resource gets its own, justified by what the stock
# figure actually represents:
#
#   iv_saline_units   0.20  bulk fluid held across wards; most of a facility's
#                           holding is committed to routine care, and only a
#                           modest reserve is free for an epidemic response
#   ns1_kits          0.25  diagnostics have shelf lives and lot constraints;
#                           a quarter of nominal holding is realistically
#                           deployable within a four-week window
#   ors_sachets       0.05  the recorded figure is a standing community-level
#                           stock measured in sachets, the great majority of
#                           which is already distributed to facilities and
#                           households rather than centrally re-allocable
#
# These are scenario assumptions of this teaching case, stated so a reviewer
# can challenge each one individually. The efficiency-equity analysis is run
# across several budget levels precisely so the recommendation does not rest
# on any single value.
USABLE_FRACTION = {
    "iv_saline_units": 0.20,
    "ns1_kits": 0.25,
    "ors_sachets": 0.05,
    "antimalarial_doses": 0.20,
}

# Resources the optimiser decides over. antimalarial_doses is excluded
# because AN-4 forecasts dengue and diarrhoeal disease, so malaria generates
# no demand and a decision variable with zero demand carries no information.
# Adding malaria to the forecast set would bring it back; the report states
# this as a scope boundary rather than hiding the empty column.
DECISION_RESOURCES = ["iv_saline_units", "ns1_kits", "ors_sachets"]


def build_scarcity_scenario(stock: pd.DataFrame, resources: list[str],
                            fractions: dict[str, float] | None = None
                            ) -> pd.DataFrame:
    """Convert recorded stock into the share available to this planning round."""
    f = fractions or USABLE_FRACTION
    s = stock.copy()
    for r in resources:
        if r in s.columns:
            s[r] = s[r] * f.get(r, 1.0)
    return s


def scenario_diagnostics(demand: pd.DataFrame, stock: pd.DataFrame,
                         resources: list[str]) -> dict:
    """
    Confirm an allocation problem is non-trivial before solving it.

    A scenario where stock already covers demand everywhere has an optimal
    objective of zero, which tells a reader nothing. Checking first turns
    that into a stated precondition instead of a surprising result.
    """
    out, total = {}, 0.0
    for r in resources:
        d = float(demand[r].sum()) if r in demand else 0.0
        s = float(stock[r].sum()) if r in stock else 0.0
        short = int((demand[r].values > stock[r].values).sum()) \
            if r in demand and r in stock else 0
        net = max(0.0, d - s)
        total += net
        out[r] = {"demand": d, "available_stock": s, "net_need": net,
                  "districts_short": short}
    out["_binding"] = total > 0
    out["_total_net_need_units"] = total
    return out
