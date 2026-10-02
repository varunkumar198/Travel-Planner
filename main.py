"""
AI Travel Planner - FastAPI + SQLite
Run:  pip install -r requirements.txt
      uvicorn main:app --reload
Open: http://127.0.0.1:8000

Pipeline (matches the architecture):
  Select region -> find destinations -> classify -> check days -> build route
  -> check travel time (5-hour rule) -> check budget -> coverage verification
  -> explain / suggest extra day -> final itinerary
"""
import copy
import json
import math
import os
import re
import sqlite3
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

DB_PATH = os.environ.get("TRAVEL_DB", "travel.db")

# ----------------------------------------------------------------- tunables
AVG_SPEED_KMPH = 45.0        # realistic average incl. traffic on Indian roads
ROAD_FACTOR = 1.3            # straight-line -> road distance
DAY_START = 8.0              # 08:00
DAY_END = 19.0               # 19:00
LUNCH_AT = 12.5
LUNCH_H = 1.0
REFRESH_H = 0.5              # rest / bath / freshen-up break after lunch
MAX_CONTINUOUS_DRIVE_H = 5.0 # 5-hour travel-break rule
BREAK_H = 0.5
IMPORTANT_LEVEL = 4          # importance >= 4 counts as "must see"
RESERVE_PCT = 0.10           # emergency reserve
CATEGORIES = ["temple", "beach", "heritage", "nature", "food"]

app = FastAPI(title="AI Travel Planner")


# ======================================================================
# DATABASE
# ======================================================================
def db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


SEED_PLACES = [
    # name, region, category, lat, lng, importance(1-5), visit_hours, entry_fee, description
    ("Padmanabhaswamy Temple", "kerala", "temple", 8.4828, 76.9436, 5, 2.0, 0, "Ancient Vishnu temple in Thiruvananthapuram"),
    ("Kovalam Beach", "kerala", "beach", 8.4004, 76.9787, 4, 2.5, 0, "Crescent-shaped beach with lighthouse"),
    ("Varkala Cliff & Beach", "kerala", "beach", 8.7379, 76.7163, 3, 2.5, 0, "Cliff-top beach with cafes"),
    ("Alleppey Backwaters", "kerala", "nature", 9.4981, 76.3388, 5, 4.0, 1200, "Houseboat / shikara cruise"),
    ("Fort Kochi", "kerala", "heritage", 9.9639, 76.2422, 4, 3.0, 0, "Chinese fishing nets, Dutch Palace, St. Francis Church"),
    ("Kumarakom Bird Sanctuary", "kerala", "nature", 9.6177, 76.4300, 3, 2.0, 100, "Vembanad lake bird sanctuary"),
    ("Munnar Tea Gardens", "kerala", "nature", 10.0889, 77.0595, 5, 4.0, 150, "Tea estates and hill viewpoints"),
    ("Periyar Wildlife Sanctuary", "kerala", "nature", 9.4667, 77.2333, 4, 3.5, 300, "Boat safari in Thekkady"),
    ("Guruvayur Temple", "kerala", "temple", 10.5944, 76.0400, 4, 1.5, 0, "Famous Krishna temple"),
    ("Kozhikode Beach Food Street", "kerala", "food", 11.2588, 75.7804, 3, 1.5, 0, "Malabar biryani and street snacks"),
    ("Basilica of Bom Jesus", "goa", "heritage", 15.5009, 73.9116, 5, 1.5, 0, "UNESCO World Heritage church"),
    ("Fort Aguada", "goa", "heritage", 15.4922, 73.7736, 4, 1.5, 0, "17th-century Portuguese fort"),
    ("Fontainhas Latin Quarter", "goa", "heritage", 15.4966, 73.8329, 3, 1.5, 0, "Colourful Portuguese-era lanes in Panjim"),
    ("Baga Beach", "goa", "beach", 15.5553, 73.7517, 4, 3.0, 0, "Lively North Goa beach"),
    ("Palolem Beach", "goa", "beach", 15.0100, 74.0232, 4, 3.0, 0, "Quiet crescent beach in South Goa"),
    ("Dudhsagar Falls", "goa", "nature", 15.3144, 74.3143, 5, 4.0, 400, "Four-tiered waterfall, jeep safari"),
    ("Mangueshi Temple", "goa", "temple", 15.4489, 73.9704, 3, 1.0, 0, "Shiva temple with lamp tower"),
    ("Anjuna Market & Shacks", "goa", "food", 15.5736, 73.7407, 3, 2.0, 0, "Seafood shacks and flea market"),
]

SEED_SERVICES = [
    # name, region, type, lat, lng, price, notes
    ("Hotel Pearl Residency", "kerala", "hotel", 8.5100, 76.9500, 1800, "Mid-range, Trivandrum"),
    ("Backwater Homestay", "kerala", "hotel", 9.4900, 76.3300, 2200, "Homestay, Alleppey"),
    ("Tea Valley Resort", "kerala", "hotel", 10.0850, 77.0600, 2600, "Munnar"),
    ("Fort Heritage Inn", "kerala", "hotel", 9.9650, 76.2450, 2400, "Fort Kochi"),
    ("Malabar Meals", "kerala", "restaurant", 9.9700, 76.2800, 350, "Kerala meals"),
    ("Sadya House", "kerala", "restaurant", 8.4900, 76.9500, 300, "Veg sadya"),
    ("HP Fuel Station Kochi", "kerala", "fuel", 9.9800, 76.2900, 0, "24x7"),
    ("IOCL Munnar", "kerala", "fuel", 10.0900, 77.0500, 0, "Petrol & diesel"),
    ("IOCL Trivandrum", "kerala", "fuel", 8.5000, 76.9500, 0, "24x7"),
    ("Kochi Auto Care", "kerala", "repair", 9.9900, 76.3000, 0, "Car repair & puncture"),
    ("Munnar Motors", "kerala", "repair", 10.0870, 77.0620, 0, "Mechanic, tyre shop"),
    ("Panjim Comfort Inn", "goa", "hotel", 15.4990, 73.8250, 2000, "Panjim"),
    ("Baga Beach Stay", "goa", "hotel", 15.5560, 73.7530, 2800, "North Goa"),
    ("Palolem Huts", "goa", "hotel", 15.0110, 74.0240, 2200, "South Goa"),
    ("Fisherman's Kitchen", "goa", "restaurant", 15.5500, 73.7500, 500, "Goan seafood"),
    ("Bharat Petroleum Mapusa", "goa", "fuel", 15.5900, 73.8100, 0, "24x7"),
    ("HP Margao", "goa", "fuel", 15.2700, 73.9600, 0, "Petrol & diesel"),
    ("Goa Wheels Garage", "goa", "repair", 15.4900, 73.8200, 0, "Car repair, towing"),
]


def init_db():
    con = db()
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS places(
            id INTEGER PRIMARY KEY, name TEXT, region TEXT, category TEXT,
            lat REAL, lng REAL, importance INTEGER, visit_hours REAL,
            entry_fee REAL, description TEXT);
        CREATE TABLE IF NOT EXISTS services(
            id INTEGER PRIMARY KEY, name TEXT, region TEXT, type TEXT,
            lat REAL, lng REAL, price REAL, notes TEXT);
        CREATE TABLE IF NOT EXISTS plans(
            id INTEGER PRIMARY KEY AUTOINCREMENT, request TEXT, result TEXT,
            created TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
        """
    )
    if con.execute("SELECT COUNT(*) FROM places").fetchone()[0] == 0:
        con.executemany(
            "INSERT INTO places(name,region,category,lat,lng,importance,visit_hours,entry_fee,description) "
            "VALUES(?,?,?,?,?,?,?,?,?)", SEED_PLACES)
        con.executemany(
            "INSERT INTO services(name,region,type,lat,lng,price,notes) VALUES(?,?,?,?,?,?,?)",
            SEED_SERVICES)
    con.commit()
    con.close()


init_db()


def get_places(regions, interests=None):
    con = db()
    q = "SELECT * FROM places WHERE region IN (%s)" % ",".join("?" * len(regions))
    rows = [dict(r) for r in con.execute(q, [r.lower() for r in regions])]
    con.close()
    if interests:
        rows = [r for r in rows if r["category"] in interests or r["importance"] >= 5]
    return rows


def get_services(regions, stype=None):
    con = db()
    q = "SELECT * FROM services WHERE region IN (%s)" % ",".join("?" * len(regions))
    args = [r.lower() for r in regions]
    if stype:
        q += " AND type=?"
        args.append(stype)
    rows = [dict(r) for r in con.execute(q, args)]
    con.close()
    return rows


def known_regions():
    con = db()
    r = [x[0] for x in con.execute("SELECT DISTINCT region FROM places")]
    con.close()
    return r


# ======================================================================
# GEO HELPERS + EXTERNAL APIS (maps / weather) - all fail-soft
# ======================================================================
def haversine_km(a, b):
    R = 6371.0
    la1, lo1, la2, lo2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    d = math.sin((la2 - la1) / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2
    return 2 * R * math.asin(math.sqrt(d))


def road_km(a, b):
    """Swap this for OSRM / Google Directions for real road distances (see osrm_km)."""
    return haversine_km(a, b) * ROAD_FACTOR


def osrm_km(a, b):
    """Optional: real routing via public OSRM demo server. Returns None on failure."""
    try:
        url = f"https://router.project-osrm.org/route/v1/driving/{a[1]},{a[0]};{b[1]},{b[0]}?overview=false"
        r = httpx.get(url, timeout=5).json()
        return r["routes"][0]["distance"] / 1000
    except Exception:
        return None


def geocode(name):
    """Nominatim geocoding; returns (lat, lng) or None."""
    if not name:
        return None
    try:
        r = httpx.get("https://nominatim.openstreetmap.org/search",
                      params={"q": name, "format": "json", "limit": 1},
                      headers={"User-Agent": "ai-travel-planner/1.0"}, timeout=5).json()
        if r:
            return float(r[0]["lat"]), float(r[0]["lon"])
    except Exception:
        pass
    return None


WMO = {0: "Clear", 1: "Mostly clear", 2: "Partly cloudy", 3: "Overcast", 45: "Fog", 51: "Light drizzle",
       61: "Light rain", 63: "Rain", 65: "Heavy rain", 80: "Showers", 81: "Heavy showers", 95: "Thunderstorm"}


def weather_forecast(lat, lng, days=7):
    """Open-Meteo forecast (no API key). Returns list of daily dicts or []."""
    try:
        r = httpx.get("https://api.open-meteo.com/v1/forecast", params={
            "latitude": lat, "longitude": lng, "timezone": "auto", "forecast_days": min(days, 16),
            "daily": "weather_code,temperature_2m_max,temperature_2m_min,precipitation_probability_max"},
            timeout=5).json()["daily"]
        out = []
        for i, d in enumerate(r["time"]):
            out.append({"date": d, "summary": WMO.get(r["weather_code"][i], "Mixed"),
                        "max_c": r["temperature_2m_max"][i], "min_c": r["temperature_2m_min"][i],
                        "rain_chance_pct": r["precipitation_probability_max"][i]})
        return out
    except Exception:
        return []


def fmt_clock(h):
    h = round(h * 60) / 60
    return f"{int(h):02d}:{int(round((h - int(h)) * 60)) % 60:02d}"


def maps_link(lat, lng):
    return f"https://www.google.com/maps/dir/?api=1&destination={lat},{lng}"


# ======================================================================
# TRIP ENGINE: day builder with 5-hour rule, lunch, and rest breaks
# ======================================================================
class DayState:
    def __init__(self, pos):
        self.clock = DAY_START
        self.pos = pos
        self.cont = 0.0          # continuous driving hours since last real stop
        self.lunch = False
        self.events = []
        self.km = 0.0
        self.stops = []

    def _push(self, kind, label, hours, **extra):
        self.events.append({"type": kind, "label": label, "start": fmt_clock(self.clock),
                            "end": fmt_clock(self.clock + hours), "hours": round(hours, 2), **extra})
        self.clock += hours

    def _lunch(self):
        self.lunch = True
        self._push("lunch", "Lunch", LUNCH_H)
        self._push("rest", "Rest / bath / freshen-up break", REFRESH_H)
        self.cont = 0.0

    def _maybe_lunch(self):
        if not self.lunch and self.clock >= LUNCH_AT:
            self._lunch()

    def drive_to(self, name, lat, lng):
        km = road_km(self.pos, (lat, lng))
        total_h = km / AVG_SPEED_KMPH
        remaining = total_h
        while remaining > 1e-9:
            self._maybe_lunch()
            room = MAX_CONTINUOUS_DRIVE_H - self.cont
            if room <= 1e-9:  # 5-hour rule triggers a break
                self._push("break", "Mandatory break (5-hour travel rule): tea, stretch, driver rest", BREAK_H)
                self.cont = 0.0
                continue
            seg = min(room, remaining)
            self._push("drive", f"Drive to {name}", seg, km=round(km * seg / total_h, 1))
            self.cont += seg
            remaining -= seg
        self.km += km
        self.pos = (lat, lng)

    def visit(self, p):
        self._maybe_lunch()
        self._push("visit", f"Visit {p['name']}", p["visit_hours"], place_id=p["id"], category=p["category"],
                   lat=p["lat"], lng=p["lng"], entry_fee=p["entry_fee"], map=maps_link(p["lat"], p["lng"]))
        self.cont = 0.0
        self.stops.append(p)

    def finalize(self):
        if not self.lunch and self.clock >= LUNCH_AT - 0.01:
            self._lunch()


def build_days(start, places, n_days):
    """Greedy value-per-hour route builder. Returns (days, leftover, reasons)."""
    remaining = list(places)
    days, reasons = [], {}
    pos = start
    for _ in range(n_days):
        if not remaining:
            break
        day = DayState(pos)
        while True:
            best, best_score = None, -1
            for p in remaining:
                trial = copy.deepcopy(day)
                trial.drive_to(p["name"], p["lat"], p["lng"])
                trial.visit(p)
                trial.finalize()
                if trial.clock > DAY_END:
                    continue
                spent = trial.clock - day.clock
                score = (p["importance"] ** 4) / max(spent, 0.25)
                if p["category"] not in {s["category"] for s in day.stops}:
                    score *= 1.15
                if score > best_score:
                    best, best_score = p, score
            if not best:
                break
            day.drive_to(best["name"], best["lat"], best["lng"])
            day.visit(best)
            remaining.remove(best)
        day.finalize()
        if not day.stops:
            break
        days.append(day)
        pos = day.pos
    # reasons for leftovers
    for p in remaining:
        d = haversine_km(pos, (p["lat"], p["lng"])) * ROAD_FACTOR / AVG_SPEED_KMPH
        if d + p["visit_hours"] > (DAY_END - DAY_START - LUNCH_H - REFRESH_H):
            reasons[p["id"]] = (f"About {d:.1f} h of driving from the end of the route; it can't fit in one day "
                                f"within the daily limit and the 5-hour break rule, so it needs a transfer day.")
        else:
            reasons[p["id"]] = "Not enough days available to include it alongside higher-value stops."
    return days, remaining, reasons


# ======================================================================
# BUDGET MANAGER
# ======================================================================
def compute_budget(req, days, regions):
    n_days = max(len(days), 1)
    nights = max(n_days - 1, 0)
    rooms = math.ceil(req.travelers / 2)
    hotels = get_services(regions, "hotel")
    hotel_rate = (sum(h["price"] for h in hotels) / len(hotels)) if hotels else 2000
    total_km = sum(d.km for d in days)
    start_pos = days[0].events[0] if days else None
    return_km = 0.0
    if days:
        return_km = getattr(days[0], "_start_pos_km", 0.0)
    activities = sum(s["entry_fee"] for d in days for s in d.stops) * req.travelers
    fuel = transport = 0.0
    all_km = total_km + return_km
    if req.transport == "car":
        fuel = all_km / req.mileage_kmpl * req.fuel_price
    else:
        transport = all_km * 2.5 * req.travelers
    lines = {
        "transport": round(transport), "fuel": round(fuel),
        "hotels": round(nights * rooms * hotel_rate),
        "food": round(n_days * req.travelers * req.food_per_person_day),
        "activities": round(activities),
    }
    subtotal = sum(lines.values())
    lines["emergency_reserve"] = round(subtotal * RESERVE_PCT)
    total = subtotal + lines["emergency_reserve"]
    return {"breakdown": lines, "total": total, "per_person": round(total / req.travelers),
            "budget": req.budget, "within_budget": total <= req.budget,
            "balance": round(req.budget - total), "total_km": round(all_km),
            "assumptions": {"rooms": rooms, "hotel_rate_per_night": round(hotel_rate), "nights": nights}}


# ======================================================================
# COVERAGE CHECKER
# ======================================================================
def coverage_check(req, regions, all_places, days, reasons, start, dropped_for_budget):
    included_ids = {s["id"] for d in days for s in d.stops}
    important = [p for p in all_places if p["importance"] >= IMPORTANT_LEVEL]
    missed = [p for p in important if p["id"] not in included_ids]
    report = {"important_total": len(important), "important_included": len(important) - len(missed),
              "missed": [], "suggestion": None}
    for p in missed:
        why = dropped_for_budget.get(p["id"]) or reasons.get(p["id"]) or "Excluded by the planner."
        alts = [s["name"] for d in days for s in d.stops if s["category"] == p["category"]][:2]
        report["missed"].append({"name": p["name"], "category": p["category"], "importance": p["importance"],
                                 "reason": why, "included_alternative_same_category": alts})
    if missed:
        # simulate extra days (ignoring budget) until every important place fits
        for extra in range(1, 6):
            d2, left, _ = build_days(start, all_places, req.days + extra)
            ids2 = {s["id"] for d in d2 for s in d.stops}
            if all(p["id"] in ids2 for p in important):
                req2 = req.model_copy(update={"days": req.days + extra})
                cost = compute_budget_with_return(req2, d2, regions, start)
                report["suggestion"] = {
                    "extra_days": extra,
                    "message": f"Add {extra} more day(s) to cover every must-see place. "
                               f"Estimated total: Rs {cost['total']:,} "
                               f"({'within' if cost['within_budget'] else 'over'} your budget by Rs {abs(cost['balance']):,}).",
                    "estimated_total": cost["total"]}
                break
        else:
            report["suggestion"] = {"extra_days": None,
                                    "message": "Even with 5 extra days some places are far apart. Split into two trips or drop far-away stops."}
    return report


def compute_budget_with_return(req, days, regions, start):
    if days:
        days[0]._start_pos_km = road_km(days[-1].pos, start)
    return compute_budget(req, days, regions)


# ======================================================================
# MAIN PLANNER
# ======================================================================
class TripRequest(BaseModel):
    start_location: str = ""
    start_lat: Optional[float] = None
    start_lng: Optional[float] = None
    regions: list[str] = Field(default_factory=lambda: ["kerala"])
    travelers: int = 2
    days: int = 3
    budget: float = 30000
    transport: str = "car"          # car | public
    mileage_kmpl: float = 15
    fuel_price: float = 105
    food_per_person_day: float = 600
    interests: Optional[list[str]] = None   # subset of CATEGORIES
    include_weather: bool = True


def nearby_services(pos, regions, per_type=1, radius_km=40):
    out = {}
    for t in ("hotel", "restaurant", "fuel", "repair"):
        items = []
        for s in get_services(regions, t):
            d = haversine_km(pos, (s["lat"], s["lng"]))
            if d <= radius_km:
                items.append({**s, "distance_km": round(d, 1), "map": maps_link(s["lat"], s["lng"])})
        out[t] = sorted(items, key=lambda x: x["distance_km"])[:per_type]
    return out


def plan_trip(req: TripRequest):
    regions = [r.lower() for r in req.regions]
    unknown = [r for r in regions if r not in known_regions()]
    if unknown:
        raise HTTPException(400, f"Unknown region(s): {unknown}. Available: {known_regions()}")
    all_places = get_places(regions, req.interests)
    if not all_places:
        raise HTTPException(400, "No places found for those regions/interests.")

    # start point: explicit coords > geocode > centroid of region
    notes = []
    if req.start_lat is not None and req.start_lng is not None:
        start = (req.start_lat, req.start_lng)
    else:
        start = geocode(req.start_location)
        if not start:
            start = (sum(p["lat"] for p in all_places) / len(all_places),
                     sum(p["lng"] for p in all_places) / len(all_places))
            notes.append("Start location could not be resolved, so the centre of the region is used as the start.")

    # build -> check budget -> drop lowest-value stop until it fits
    working = list(all_places)
    dropped = {}
    while True:
        days, left, reasons = build_days(start, working, req.days)
        cost = compute_budget_with_return(req, days, regions, start)
        if cost["within_budget"] or len(working) <= 1 or not days:
            break
        victim = min((s for d in days for s in d.stops), key=lambda s: (s["importance"], -s["entry_fee"]))
        dropped[victim["id"]] = "Removed to keep the trip within your budget (lowest-value stop on the route)."
        working = [p for p in working if p["id"] != victim["id"]]

    coverage = coverage_check(req, regions, all_places, days, reasons, start, dropped)
    optional_left = [p["name"] for p in all_places if p["importance"] < IMPORTANT_LEVEL
                     and p["id"] not in {s["id"] for d in days for s in d.stops}]

    weather = []
    if req.include_weather and days:
        weather = weather_forecast(days[0].stops[0]["lat"], days[0].stops[0]["lng"], len(days))

    out_days = []
    for i, d in enumerate(days):
        hotel_stop = nearby_services(d.pos, regions)
        night = None
        if i < len(days) - 1 and hotel_stop["hotel"]:
            night = hotel_stop["hotel"][0]
        out_days.append({
            "day": i + 1,
            "summary": ", ".join(s["name"] for s in d.stops),
            "distance_km": round(d.km),
            "drive_hours": round(sum(e["hours"] for e in d.events if e["type"] == "drive"), 1),
            "events": d.events,
            "overnight_stay": night,
            "en_route_support": nearby_services(d.pos, regions, per_type=2),
            "weather": weather[i] if i < len(weather) else None,
        })

    return {
        "request": req.model_dump(),
        "days_planned": len(days),
        "days_requested": req.days,
        "itinerary": out_days,
        "budget": cost,
        "coverage": coverage,
        "optional_places_not_included": optional_left,
        "notes": notes + ([f"Trip can be completed in {len(days)} day(s), fewer than the {req.days} requested."]
                          if len(days) < req.days else []),
        "return_to_start_note": "Return travel to the starting point is included in fuel/transport cost but is not "
                                "scheduled inside the daily plan.",
    }


# ======================================================================
# AI LAYER: natural-language requests (rule-based, LLM optional)
# ======================================================================
def _num(s, unit=None):
    v = float(s.replace(",", ""))
    if unit == "k":
        v *= 1000
    elif unit in ("lakh", "lac", "l"):
        v *= 100000
    return v


def rule_parse(msg: str, req: dict):
    t = msg.lower()
    ch = {}
    m = re.search(r"(\d+)\s*(?:days?|nights?)", t)
    if m:
        ch["days"] = int(m.group(1))
    if re.search(r"(one|1|an?)\s+(more|extra|additional)\s+day|add (a |one )?day", t):
        ch["days"] = req["days"] + 1
    if re.search(r"(one|1)\s+(less|fewer)\s+day|remove (a |one )?day|reduce (a |one )?day", t):
        ch["days"] = max(1, req["days"] - 1)
    m = re.search(r"(increase|raise|add).{0,15}budget.{0,10}?(?:by)?\s*(?:rs\.?|₹)?\s*(\d[\d,]*)\s*(k|lakh|lac)?", t)
    if m:
        ch["budget"] = req["budget"] + _num(m.group(2), m.group(3))
    m = re.search(r"(reduce|cut|lower|decrease).{0,15}budget.{0,10}?(?:by)?\s*(?:rs\.?|₹)?\s*(\d[\d,]*)\s*(k|lakh|lac)?", t)
    if m:
        ch["budget"] = max(1000, req["budget"] - _num(m.group(2), m.group(3)))
    if "budget" not in ch:
        m = re.search(r"budget[^\d]{0,15}(\d[\d,]*)\s*(k|lakh|lac)?", t) or \
            re.search(r"(?:rs\.?|₹|inr)\s*(\d[\d,]*)\s*(k|lakh|lac)?", t)
        if m:
            ch["budget"] = _num(m.group(1), m.group(2))
    if any(w in t for w in ("cheaper", "low budget", "save money", "cut cost")) and "budget" not in ch:
        ch["budget"] = round(req["budget"] * 0.8)
    m = re.search(r"(\d+)\s*(?:people|persons?|travell?ers|adults|members|friends)", t)
    if m:
        ch["travelers"] = int(m.group(1))
    regs = [r for r in known_regions() if r in t]
    if regs:
        ch["regions"] = regs
    if re.search(r"\b(only|more|focus|mostly|just)\b", t):
        ints = [c for c in CATEGORIES if c in t]
        if ints:
            ch["interests"] = ints
    if "public transport" in t or "bus" in t or "train" in t:
        ch["transport"] = "public"
    if re.search(r"\b(own car|drive|by car)\b", t):
        ch["transport"] = "car"
    return ch


def llm_parse(msg: str, req: dict):
    """Optional: set ANTHROPIC_API_KEY to let Claude extract changes. Falls back to rules."""
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        return None
    try:
        prompt = ("Current trip request JSON: " + json.dumps(req) + "\nUser message: " + msg +
                  "\nReturn ONLY a JSON object with any of these keys that the user wants changed: "
                  "days(int), budget(number), travelers(int), regions(list of lowercase names from "
                  f"{known_regions()}), interests(list from {CATEGORIES}), transport('car'|'public'). "
                  "Return {} if the message is only a question.")
        r = httpx.post("https://api.anthropic.com/v1/messages",
                       headers={"x-api-key": key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                       json={"model": "claude-sonnet-5-5", "max_tokens": 300,
                             "messages": [{"role": "user", "content": prompt}]}, timeout=20).json()
        text = "".join(b.get("text", "") for b in r["content"])
        return json.loads(re.sub(r"```json|```", "", text).strip())
    except Exception:
        return None


def answer_question(msg: str, result: dict):
    t = msg.lower()
    regions = result["request"]["regions"]
    last_pos = None
    if result["itinerary"]:
        ev = [e for e in result["itinerary"][-1]["events"] if e["type"] == "visit"]
        if ev:
            last_pos = (ev[-1]["lat"], ev[-1]["lng"])
    mapping = {"fuel": ("fuel", "petrol", "pump", "diesel"), "repair": ("repair", "mechanic", "garage", "puncture"),
               "hotel": ("hotel", "stay", "room"), "restaurant": ("restaurant", "eat", "food", "lunch")}
    for stype, words in mapping.items():
        if any(w in t for w in words) and "?" in t or (stype == "fuel" and "petrol" in t):
            if last_pos:
                s = nearby_services(last_pos, regions, per_type=3, radius_km=80)[stype]
                if s:
                    return f"Nearest {stype} options to your last stop: " + "; ".join(
                        f"{x['name']} ({x['distance_km']} km)" for x in s)
            return f"No {stype} listing found near the route in the database yet."
    if "why" in t or "missing" in t or "skipped" in t or "left out" in t:
        m = result["coverage"]["missed"]
        if not m:
            return "All must-see places are covered in this itinerary."
        for x in m:
            if x["name"].lower().split()[0] in t:
                return f"{x['name']}: {x['reason']}"
        s = result["coverage"]["suggestion"]
        return "Missed: " + "; ".join(f"{x['name']} ({x['reason']})" for x in m) + \
               (f" Suggestion: {s['message']}" if s else "")
    if "weather" in t or "rain" in t:
        w = [(d["day"], d["weather"]) for d in result["itinerary"] if d["weather"]]
        if not w:
            return "Weather data is unavailable right now (no internet or API limit)."
        return "; ".join(f"Day {d}: {x['summary']}, {x['min_c']}-{x['max_c']} C, rain {x['rain_chance_pct']}%" for d, x in w)
    if "budget" in t and ("break" in t or "cost" in t or "much" in t):
        b = result["budget"]
        return f"Total Rs {b['total']:,} (Rs {b['per_person']:,} per person). Breakdown: " + \
               ", ".join(f"{k} Rs {v:,}" for k, v in b["breakdown"].items())
    return None


# ======================================================================
# API
# ======================================================================
def save_plan(req, result):
    con = db()
    cur = con.execute("INSERT INTO plans(request,result) VALUES(?,?)", (json.dumps(req), json.dumps(result)))
    con.commit()
    pid = cur.lastrowid
    con.close()
    return pid


def load_plan(pid):
    con = db()
    r = con.execute("SELECT request,result FROM plans WHERE id=?", (pid,)).fetchone()
    con.close()
    if not r:
        raise HTTPException(404, "Plan not found")
    return json.loads(r["request"]), json.loads(r["result"])


@app.post("/plan")
def create_plan(req: TripRequest):
    result = plan_trip(req)
    result["plan_id"] = save_plan(req.model_dump(), result)
    return result


@app.get("/plan/{plan_id}")
def get_plan(plan_id: int):
    return load_plan(plan_id)[1]


class ChatIn(BaseModel):
    message: str
    plan_id: Optional[int] = None


@app.post("/chat")
def chat(body: ChatIn):
    if body.plan_id:
        req_dict, result = load_plan(body.plan_id)
    else:
        req_dict, result = TripRequest().model_dump(), None

    changes = llm_parse(body.message, req_dict) or rule_parse(body.message, req_dict)
    changes = {k: v for k, v in changes.items() if k in TripRequest.model_fields}

    if changes or result is None:
        new_req = TripRequest(**{**req_dict, **changes})
        new_result = plan_trip(new_req)
        new_result["plan_id"] = save_plan(new_req.model_dump(), new_result)
        b, c = new_result["budget"], new_result["coverage"]
        reply = (f"Updated plan ({', '.join(f'{k}={v}' for k, v in changes.items()) or 'defaults'}). "
                 f"{new_result['days_planned']} day(s), total Rs {b['total']:,} "
                 f"({'within' if b['within_budget'] else 'OVER'} budget). "
                 f"Must-see coverage: {c['important_included']}/{c['important_total']}.")
        if c["suggestion"]:
            reply += " " + c["suggestion"]["message"]
        return {"reply": reply, "plan": new_result}

    ans = answer_question(body.message, result)
    return {"reply": ans or "I can change days, budget, travelers, region or interests, and answer questions about "
                            "skipped places, weather, fuel, repair shops, hotels, restaurants and budget.",
            "plan": result}


@app.get("/places")
def list_places(region: Optional[str] = None, category: Optional[str] = None):
    con = db()
    q, a = "SELECT * FROM places WHERE 1=1", []
    if region:
        q += " AND region=?"; a.append(region.lower())
    if category:
        q += " AND category=?"; a.append(category.lower())
    rows = [dict(r) for r in con.execute(q + " ORDER BY importance DESC", a)]
    con.close()
    return rows


@app.get("/nearby")
def nearby(lat: float, lng: float, type: Optional[str] = None, radius_km: float = 30):
    regions = known_regions()
    res = []
    for s in get_services(regions, type):
        d = haversine_km((lat, lng), (s["lat"], s["lng"]))
        if d <= radius_km:
            res.append({**s, "distance_km": round(d, 1), "map": maps_link(s["lat"], s["lng"])})
    return sorted(res, key=lambda x: x["distance_km"])


@app.get("/weather")
def weather(lat: float, lng: float, days: int = 7):
    return weather_forecast(lat, lng, days)


@app.get("/regions")
def regions():
    return known_regions()


# ======================================================================
# WEB INTERFACE
# ======================================================================
PAGE = """<!doctype html><html><head><meta charset="utf-8"><title>AI Travel Planner</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
body{font-family:system-ui,sans-serif;max-width:980px;margin:0 auto;padding:16px;background:#f6f7f9;color:#1c1f24}
h1{margin:.2em 0}.card{background:#fff;border-radius:10px;padding:14px;margin:12px 0;box-shadow:0 1px 3px #0002}
label{display:inline-block;margin:4px 10px 4px 0;font-size:13px}input,select,button{padding:7px;border-radius:6px;border:1px solid #bbb}
button{background:#0b6cff;color:#fff;border:0;cursor:pointer}.ev{padding:3px 0;border-bottom:1px dotted #ddd;font-size:14px}
.drive{color:#555}.break,.rest,.lunch{color:#a05a00}.visit{font-weight:600}.bad{color:#c00}.ok{color:#080}
#chatlog{max-height:200px;overflow:auto;font-size:14px}small{color:#666}a{color:#0b6cff}
</style></head><body>
<h1>AI Travel Planner</h1>
<div class="card"><form id="f">
<label>Start location <input name="start_location" placeholder="e.g. Bengaluru"></label>
<label>Region <select name="region"></select></label>
<label>Travelers <input name="travelers" type="number" value="2" min="1" style="width:60px"></label>
<label>Days <input name="days" type="number" value="3" min="1" style="width:60px"></label>
<label>Budget (Rs) <input name="budget" type="number" value="30000" style="width:100px"></label>
<label>Transport <select name="transport"><option>car</option><option>public</option></select></label>
<button>Plan trip</button></form></div>
<div class="card"><b>Ask / adjust</b> <small>e.g. "add one more day", "reduce budget by 5000", "why is Munnar missing?", "petrol pump nearby?", "weather?"</small><br>
<div id="chatlog"></div><input id="msg" style="width:75%"> <button id="send">Send</button></div>
<div id="out"></div>
<script>
let planId=null;const $=s=>document.querySelector(s);
fetch('/regions').then(r=>r.json()).then(rs=>$('[name=region]').innerHTML=rs.map(r=>`<option>${r}</option>`).join(''));
function render(p){
 const b=p.budget,c=p.coverage;let h='';
 h+=`<div class="card"><b>Budget</b>: Rs ${b.total.toLocaleString()} of Rs ${b.budget.toLocaleString()} <span class="${b.within_budget?'ok':'bad'}">(${b.within_budget?'within':'over'} budget, balance Rs ${b.balance.toLocaleString()})</span><br>Per person: Rs ${b.per_person.toLocaleString()} &middot; ~${b.total_km} km<br><small>`+
 Object.entries(b.breakdown).map(([k,v])=>`${k}: Rs ${v.toLocaleString()}`).join(' | ')+`</small></div>`;
 h+=`<div class="card"><b>Coverage</b>: ${c.important_included}/${c.important_total} must-see places`;
 c.missed.forEach(m=>h+=`<div class="bad">Missing: ${m.name} (${m.category}) - ${m.reason}</div>`);
 if(c.suggestion)h+=`<div><b>Suggestion:</b> ${c.suggestion.message}</div>`;
 (p.notes||[]).forEach(n=>h+=`<div><small>${n}</small></div>`);h+='</div>';
 p.itinerary.forEach(d=>{
  h+=`<div class="card"><b>Day ${d.day}</b>: ${d.summary} <small>(${d.distance_km} km, ${d.drive_hours} h driving)</small>`;
  if(d.weather)h+=`<div><small>Weather: ${d.weather.summary}, ${d.weather.min_c}-${d.weather.max_c} C, rain ${d.weather.rain_chance_pct}%</small></div>`;
  d.events.forEach(e=>h+=`<div class="ev ${e.type}">${e.start}-${e.end} &nbsp; ${e.label}${e.map?` <a href="${e.map}" target="_blank">map</a>`:''}${e.entry_fee?` <small>(Rs ${e.entry_fee}/person)</small>`:''}</div>`);
  if(d.overnight_stay)h+=`<div class="ev">Overnight: ${d.overnight_stay.name} (~Rs ${d.overnight_stay.price}/room)</div>`;
  const s=d.en_route_support;
  h+=`<small>Nearby: fuel ${s.fuel.map(x=>x.name).join(', ')||'-'} | repair ${s.repair.map(x=>x.name).join(', ')||'-'} | food ${s.restaurant.map(x=>x.name).join(', ')||'-'}</small></div>`;
 });
 $('#out').innerHTML=h;planId=p.plan_id;
}
$('#f').onsubmit=async e=>{e.preventDefault();const f=new FormData(e.target);
 const body={start_location:f.get('start_location'),regions:[f.get('region')],travelers:+f.get('travelers'),days:+f.get('days'),budget:+f.get('budget'),transport:f.get('transport')};
 const r=await fetch('/plan',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
 r.ok?render(await r.json()):alert((await r.json()).detail);};
async function send(){const m=$('#msg').value.trim();if(!m)return;$('#msg').value='';
 $('#chatlog').innerHTML+=`<div><b>You:</b> ${m}</div>`;
 const r=await fetch('/chat',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({message:m,plan_id:planId})});
 const j=await r.json();$('#chatlog').innerHTML+=`<div><b>AI:</b> ${j.reply||j.detail}</div>`;
 if(j.plan)render(j.plan);$('#chatlog').scrollTop=1e6;}
$('#send').onclick=send;$('#msg').onkeydown=e=>{if(e.key==='Enter')send()};
</script></body></html>"""


@app.get("/", response_class=HTMLResponse)
def home():
    return PAGE
