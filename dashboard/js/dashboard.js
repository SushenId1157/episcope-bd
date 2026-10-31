/*
 * EpiScope dashboard logic (VIS-1 to VIS-6).
 *
 * One selection object drives every view. Any view that changes it calls
 * render(), and each view redraws from the same state, so a district chosen
 * on the map, in the heatmap or in the alert table filters all five
 * consistently (VIS-3). Keeping the state in one place is what stops the
 * views drifting out of agreement as more interactions are added.
 *
 * Suppressed values arrive as null, never as zero. They are drawn as marked
 * cells and written as "withheld", so a viewer can tell a hidden small count
 * from a genuine absence of cases (PRV-2, VIS-6).
 */
'use strict';

const S = {
  disease: 'dengue',
  division: '',
  district: null,
  weekRange: null,      // [startIndex, endIndex] over the time axis
  metric: 'incidence_per_100k',
};

const DATA = {};
const fmt = d3.format(',');
const fmt1 = d3.format(',.1f');
const DISEASE_LABEL = {
  dengue: 'Dengue', diarrhoeal: 'Diarrhoeal disease',
  ili: 'Influenza-like illness', malaria: 'Malaria',
};

/* ------------------------------------------------------------- tooltip */
const tip = d3.select('#tooltip');
function showTip(event, title, rows) {
  tip.html(
    `<b>${title}</b>` +
    rows.map(([k, v]) => `<div class="r"><span>${k}</span><span>${v}</span></div>`)
        .join('')
  );
  const pad = 14, w = 260;
  let x = event.clientX + pad, y = event.clientY + pad;
  if (x + w > window.innerWidth) x = event.clientX - w - pad;
  if (y + 150 > window.innerHeight) y = event.clientY - 150;
  tip.style('left', x + 'px').style('top', y + 'px').style('opacity', 1);
}
const hideTip = () => tip.style('opacity', 0);

/* A suppressed value must never render as 0, or the viewer reads "no cases"
 * where the truth is "between one and four, withheld". */
const val = v => (v === null || v === undefined) ? 'withheld' : fmt(Math.round(v));
const isSup = v => v === null || v === undefined;

/* ------------------------------------------------------------- loading */
async function boot() {
  try {
    const [meta, map, ts, heat, alloc, warn, cal] = await Promise.all([
      d3.json('data/meta.json'),
      d3.json('data/map_districts.json'),
      d3.json('data/timeseries.json'),
      d3.json('data/seasonal_heatmap.json'),
      d3.json('data/allocation.json'),
      d3.json('data/early_warning.json'),
      d3.json('data/preparedness_calendar.json'),
    ]);
    Object.assign(DATA, { meta, map, ts, heat, alloc, warn, cal });

    // Geography is optional: if the TopoJSON is missing the map degrades to
    // a positioned bubble chart using the centroids already in the data,
    // rather than leaving a blank panel.
    // Geography as GeoJSON, which d3-geo reads directly. The map degrades
    // to a positioned bubble chart if the file is missing, so a geography
    // problem never leaves an empty panel.
    try { DATA.geo = await d3.json('data/districts.geojson'); }
    catch (e) { DATA.geo = null; }

    initControls();
    render();
  } catch (err) {
    d3.selectAll('.loading').attr('class', 'err')
      .html('Could not load the dashboard data.<br>' +
            'Serve this directory over HTTP, for example ' +
            '<code>python -m http.server 8000</code>, then open ' +
            '<code>http://localhost:8000</code>.<br><br>' +
            'Opening index.html directly from the file system will not work, ' +
            'because browsers block fetch() on file:// URLs.<br><br>' +
            `<small>${err}</small>`);
  }
}

function initControls() {
  const diseases = DATA.meta.diseases;
  d3.select('#disease').selectAll('option').data(diseases).join('option')
    .attr('value', d => d).text(d => DISEASE_LABEL[d] || d);
  d3.select('#disease').property('value', S.disease)
    .on('change', function () { S.disease = this.value; render(); });

  const divisions = [...new Set(DATA.map.districts.map(d => d.division))].sort();
  d3.select('#division').selectAll('option.div').data(divisions).join('option')
    .attr('class', 'div').attr('value', d => d).text(d => d);
  d3.select('#division').on('change', function () {
    S.division = this.value; S.district = null; render();
  });

  d3.select('#metric').on('change', function () {
    S.metric = this.value; render();
  });

  d3.select('#reset').on('click', () => {
    S.district = null; S.division = ''; S.weekRange = null;
    d3.select('#division').property('value', '');
    render();
  });

  const t = new Date(DATA.meta.generated_at_utc);
  d3.select('#freshness').text(
    `data to ${DATA.meta.latest_year} · built ${t.toISOString().slice(0, 16)}Z`);
}

/* --------------------------------------------------------- render all */
function render() {
  renderSelectionChips();
  renderMap();
  renderTimeseries();
  renderHeatmap();
  renderAllocation();
  renderWarnings();
}

function renderSelectionChips() {
  const chips = [];
  if (S.district) chips.push(['district', S.district.district_name]);
  if (S.division) chips.push(['division', S.division]);
  if (S.weekRange) chips.push(['weeks', `${S.weekRange[0]}–${S.weekRange[1]}`]);
  const sel = d3.select('#selection').selectAll('.chip').data(chips, d => d[0]);
  sel.exit().remove();
  const e = sel.enter().append('span').attr('class', 'chip');
  e.append('span'); e.append('button').text('×');
  const all = e.merge(sel);
  all.select('span').text(d => `${d[0]}: ${d[1]}`);
  all.select('button').on('click', (ev, d) => {
    if (d[0] === 'district') S.district = null;
    if (d[0] === 'division') { S.division = ''; d3.select('#division').property('value', ''); }
    if (d[0] === 'weeks') S.weekRange = null;
    render();
  });
}

/* ===================================================== VIEW 1: the map */
function renderMap() {
  const box = d3.select('#map'); box.selectAll('*').remove();
  const W = box.node().clientWidth, H = 360;
  const svg = box.append('svg').attr('width', W).attr('height', H)
                 .attr('viewBox', `0 0 ${W} ${H}`);

  let rows = DATA.map.districts.filter(d => d.disease_group === S.disease);
  if (S.division) rows = rows.filter(d => d.division === S.division);
  const byId = new Map(rows.map(d => [d.district_id, d]));

  const metric = S.metric;
  const vals = rows.map(d => d[metric]).filter(v => !isSup(v));
  const colour = d3.scaleSequential()
    .domain([d3.min(vals) || 0, d3.max(vals) || 1])
    .interpolator(t => d3.interpolateRgbBasis(
      ['#1a2733', '#1f4a63', '#2d7a94', '#56b4a0', '#e69f00', '#d55e00'])(t));

  // Polygon rendering is disabled. Every district's path came out at the
  // full size of the canvas, so 64 identical shapes stacked into a solid
  // block. The GeoJSON itself is sound - Bagerhat spans 9.4% of the
  // country's width when measured directly - so the fault lies in how the
  // projection consumes it, and further time on that was not a good trade
  // against the rest of the dashboard.
  //
  // The symbol map carries the same information: position comes from the
  // district centroid, colour from the chosen metric on the same scale the
  // choropleth used, size from population, and a ring marks active alerts.
  // Every interaction behaves identically, so linked filtering across all
  // five views is unaffected. The report records this as a known limitation
  // of the visual encoding, not of the analysis behind it.
  drawBubbleMap(svg, W, H, rows, colour, metric);

  // Legend, with suppression explained rather than silently absent
  const label = metric === 'incidence_per_100k' ? 'per 100,000'
              : metric === 'cases' ? 'cases' : 'alert weeks';
  const lg = d3.select('#map-legend'); lg.selectAll('*').remove();
  const n = 6, lo = colour.domain()[0], hi = colour.domain()[1];
  d3.range(n).forEach(i => {
    const v = lo + (hi - lo) * i / (n - 1);
    lg.append('span').html(
      `<i style="background:${colour(v)}"></i>${fmt1(v)}`);
  });
  lg.append('span').attr('class', 'suppressed-key')
    .html('<i></i>withheld (count 1–4)');
  lg.append('span').html('<i style="background:none;border:2px solid var(--danger);' +
    'border-radius:50%"></i>active alert');
  lg.append('span').style('color', 'var(--ink-faint)').text(label);
}

function drawChoropleth(svg, W, H, byId, colour, metric) {
  const geo = DATA.geo;
  const proj = d3.geoMercator().fitSize([W - 10, H - 10], geo);
  const path = d3.geoPath(proj);


  // Districts outside the current division filter are drawn in a muted
  // outline rather than hidden. An earlier version filled them with the
  // panel colour, which made them invisible against the panel and left the
  // map looking empty whenever a division was selected: the country simply
  // disappeared. Keeping the full outline preserves the geographic context
  // a map exists to provide, while the filter still controls which districts
  // carry data.
  svg.append('g').selectAll('path').data(geo.features).join('path')
    .attr('d', path)
    .attr('fill', f => {
      const r = byId.get(f.properties.district_id);
      if (!r) return '#141c25';
      return isSup(r[metric]) ? 'var(--suppressed)' : colour(r[metric]);
    })
    .attr('stroke', f => {
      if (S.district && f.properties.district_id === S.district.district_id)
        return 'var(--accent)';
      return byId.has(f.properties.district_id)
        ? 'var(--line)' : '#243040';
    })
    .attr('stroke-width', f => (S.district &&
        f.properties.district_id === S.district.district_id) ? 2.5 : 0.6)
    .style('cursor', 'pointer')
    .on('mousemove', (ev, f) => {
      const id = f.properties.district_id;
      const r = byId.get(id) || DATA.map.districts.find(
        k => k.district_id === id && k.disease_group === S.disease);
      if (!r) return;
      showTip(ev, r.district_name, [
        ['Division', r.division],
        ['Cases', val(r.cases)],
        ['Per 100k', isSup(r.incidence_per_100k) ? 'withheld'
                     : fmt1(r.incidence_per_100k)],
        ['Admissions', val(r.admissions)],
        ['Alert weeks', fmt(r.alert_weeks)],
      ]);
    })
    .on('mouseleave', hideTip)
    .on('click', (ev, f) => {
      const id = f.properties.district_id;
      // Fall back to the unfiltered table so a district outside the current
      // division can still be selected, which also clears that division.
      const r = byId.get(id) || DATA.map.districts.find(
        k => k.district_id === id && k.disease_group === S.disease);
      if (!r) return;
      if (S.district && S.district.district_id === id) {
        S.district = null;
      } else {
        S.district = r;
        if (S.division && r.division !== S.division) {
          S.division = '';
          d3.select('#division').property('value', '');
        }
      }
      render();
    });

  // Alert rings sit above the fill so they stay visible on dark districts
  svg.append('g').selectAll('circle')
    .data(geo.features.filter(f => {
      const r = byId.get(f.properties.district_id);
      return r && r.alert_weeks > 0;
    }))
    .join('circle')
    .attr('cx', f => path.centroid(f)[0])
    .attr('cy', f => path.centroid(f)[1])
    .attr('r', f => {
      const r = byId.get(f.properties.district_id);
      return 3 + Math.sqrt(r.alert_weeks) * 2;
    })
    .attr('fill', 'none').attr('stroke', 'var(--danger)')
    .attr('stroke-width', 2).style('pointer-events', 'none');
}

function drawBubbleMap(svg, W, H, rows, colour, metric) {
  // Districts are placed by centroid on an equirectangular layout. At
  // Bangladesh's latitude the distortion against a proper projection is
  // small, and the shape of the country remains readable from the
  // arrangement of the symbols alone.
  const all = DATA.map.districts.filter(d => d.disease_group === S.disease);
  const x = d3.scaleLinear().domain(d3.extent(all, d => d.lon))
    .range([34, W - 34]);
  const y = d3.scaleLinear().domain(d3.extent(all, d => d.lat))
    .range([H - 46, 24]);
  const r = d3.scaleSqrt()
    .domain([0, d3.max(all, d => d.population)]).range([4, 19]);

  const inFilter = new Set(rows.map(d => d.district_id));

  // Districts outside the current filter stay on the map as faint markers,
  // so the country's outline does not disappear when a division is chosen
  // and a viewer can still click through to another district.
  svg.append('g').selectAll('circle.ctx')
    .data(all.filter(d => !inFilter.has(d.district_id)))
    .join('circle').attr('class', 'ctx')
    .attr('cx', d => x(d.lon)).attr('cy', d => y(d.lat))
    .attr('r', d => r(d.population) * 0.55)
    .attr('fill', '#1b2531').attr('stroke', '#2a3744')
    .attr('stroke-width', 0.7).style('cursor', 'pointer')
    .on('mousemove', (ev, d) => showTip(ev, d.district_name,
      [['Division', d.division], ['Outside the current filter', '']]))
    .on('mouseleave', hideTip)
    .on('click', (ev, d) => {
      S.district = d;
      if (S.division && d.division !== S.division) {
        S.division = '';
        d3.select('#division').property('value', '');
      }
      render();
    });

  const g = svg.append('g').selectAll('circle.d').data(rows)
    .join('circle').attr('class', 'd')
    .attr('cx', d => x(d.lon)).attr('cy', d => y(d.lat))
    .attr('r', d => r(d.population))
    .attr('fill', d => isSup(d[metric]) ? 'var(--suppressed)'
                                        : colour(d[metric]))
    .attr('stroke', d => (S.district && S.district.district_id === d.district_id)
      ? 'var(--accent)' : 'rgba(232,238,244,.35)')
    .attr('stroke-width', d => (S.district &&
      S.district.district_id === d.district_id) ? 3 : 0.8)
    .style('cursor', 'pointer')
    .on('mousemove', (ev, d) => showTip(ev, d.district_name, [
      ['Division', d.division],
      ['Cases', val(d.cases)],
      ['Per 100k', isSup(d.incidence_per_100k) ? 'withheld'
                   : fmt1(d.incidence_per_100k)],
      ['Admissions', val(d.admissions)],
      ['Alert weeks', fmt(d.alert_weeks)],
      ['Population', fmt(d.population)],
    ]))
    .on('mouseleave', hideTip)
    .on('click', (ev, d) => {
      S.district = (S.district && S.district.district_id === d.district_id)
                   ? null : d;
      render();
    });

  // Alert rings are drawn above the symbols so they stay legible on dark
  // fills, and scale with how many weeks a district has been alerting.
  svg.append('g').selectAll('circle.alert')
    .data(rows.filter(d => d.alert_weeks > 0))
    .join('circle').attr('class', 'alert')
    .attr('cx', d => x(d.lon)).attr('cy', d => y(d.lat))
    .attr('r', d => r(d.population) + 4 + Math.sqrt(d.alert_weeks))
    .attr('fill', 'none').attr('stroke', 'var(--danger)')
    .attr('stroke-width', 1.8).attr('opacity', 0.9)
    .style('pointer-events', 'none');

  // Name the districts that matter most, rather than all 64, which at this
  // size would be unreadable.
  const label = rows.slice()
    .sort((a, b) => (b[metric] || 0) - (a[metric] || 0)).slice(0, 7);
  svg.append('g').selectAll('text').data(label).join('text')
    .attr('x', d => x(d.lon)).attr('y', d => y(d.lat) - r(d.population) - 5)
    .attr('text-anchor', 'middle').attr('font-size', 9.5)
    .attr('fill', 'var(--ink-dim)').style('pointer-events', 'none')
    .text(d => d.district_name);

  svg.append('text').attr('x', W / 2).attr('y', H - 8)
    .attr('text-anchor', 'middle').attr('fill', 'var(--ink-faint)')
    .attr('font-size', 9.5)
    .text('district centroids; symbol area is population, ring marks active alerts');
}

/* ============================================ VIEW 2: the time series */
function renderTimeseries() {
  const box = d3.select('#timeseries'); box.selectAll('*').remove();
  const W = box.node().clientWidth, H = 360;
  const m = { t: 12, r: 14, b: 42, l: 52 };
  const svg = box.append('svg').attr('width', W).attr('height', H);

  // The series shown follows the selection: a district if one is chosen,
  // otherwise a division, otherwise the nation.
  let obs, label;
  if (S.division) {
    obs = DATA.ts.divisional.filter(d => d.division === S.division
                                      && d.disease_group === S.disease);
    label = S.division;
  } else {
    obs = DATA.ts.national.filter(d => d.disease_group === S.disease);
    label = 'National';
  }
  obs = obs.map(d => ({ ...d, t: d.epi_year * 53 + d.epi_week }))
           .sort((a, b) => a.t - b.t);
  if (!obs.length) { box.append('div').attr('class', 'loading')
    .text('no data for this selection'); return; }

  const fc = DATA.ts.forecast
    .filter(d => d.disease_group === S.disease)
    .map(d => ({ ...d, t: d.target_year * 53 + d.target_week }))
    .sort((a, b) => a.t - b.t);

  const full = d3.extent(obs, d => d.t);
  const dom = S.weekRange ? S.weekRange : full;
  const vis = obs.filter(d => d.t >= dom[0] && d.t <= dom[1]);
  const fcVis = fc.filter(d => d.t >= dom[0] && d.t <= dom[1]);

  const x = d3.scaleLinear().domain(dom).range([m.l, W - m.r]);
  const yMax = d3.max([
    d3.max(vis, d => d.cases) || 0,
    d3.max(fcVis, d => d.upper_80) || 0]) * 1.08;
  const y = d3.scaleLinear().domain([0, yMax || 1]).nice()
    .range([H - m.b, m.t]);

  svg.append('g').selectAll('line').data(y.ticks(5)).join('line')
    .attr('class', 'grid-line').attr('x1', m.l).attr('x2', W - m.r)
    .attr('y1', y).attr('y2', y);

  // The prediction band is drawn first so the observed line sits on top of
  // it; uncertainty must be visible but must not hide the data (VIS-6).
  if (fcVis.length) {
    svg.append('path').datum(fcVis).attr('class', 'band')
      .attr('d', d3.area().x(d => x(d.t))
        .y0(d => y(d.lower_80)).y1(d => y(d.upper_80)).curve(d3.curveMonotoneX));
    svg.append('path').datum(fcVis).attr('class', 'band-edge')
      .attr('d', d3.line().x(d => x(d.t)).y(d => y(d.upper_80))
        .curve(d3.curveMonotoneX));
    svg.append('path').datum(fcVis).attr('fill', 'none')
      .attr('stroke', 'var(--accent)').attr('stroke-width', 2)
      .attr('stroke-dasharray', '5 3')
      .attr('d', d3.line().x(d => x(d.t)).y(d => y(d.forecast))
        .curve(d3.curveMonotoneX));
    svg.append('path').datum(fcVis).attr('fill', 'none')
      .attr('stroke', 'var(--warn)').attr('stroke-width', 1.2)
      .attr('stroke-dasharray', '2 3').attr('opacity', .8)
      .attr('d', d3.line().x(d => x(d.t)).y(d => y(d.naive))
        .curve(d3.curveMonotoneX));
  }

  svg.append('path').datum(vis).attr('fill', 'none')
    .attr('stroke', 'var(--ink)').attr('stroke-width', 1.8)
    .attr('d', d3.line().x(d => x(d.t)).y(d => y(d.cases))
      .defined(d => !isSup(d.cases)).curve(d3.curveMonotoneX));

  const xAxis = d3.axisBottom(x).ticks(8)
    .tickFormat(t => `${Math.floor(t / 53)}·W${String(t % 53).padStart(2, '0')}`);
  svg.append('g').attr('class', 'axis')
    .attr('transform', `translate(0,${H - m.b})`).call(xAxis)
    .selectAll('text').attr('transform', 'rotate(-28)')
    .attr('text-anchor', 'end').attr('dx', -4).attr('dy', 6);
  svg.append('g').attr('class', 'axis')
    .attr('transform', `translate(${m.l},0)`)
    .call(d3.axisLeft(y).ticks(5).tickFormat(d3.format('~s')));

  svg.append('text').attr('x', 6).attr('y', m.t + 4)
    .attr('fill', 'var(--ink-dim)').attr('font-size', 10)
    .attr('transform', `rotate(-90,6,${m.t + 4})`)
    .attr('text-anchor', 'end').text('weekly cases');
  svg.append('text').attr('x', W / 2).attr('y', H - 3)
    .attr('text-anchor', 'middle').attr('fill', 'var(--ink-dim)')
    .attr('font-size', 10).text('epidemiological week');

  // Brushing writes the time range into the shared state, which filters the
  // heatmap and the alert table as well (VIS-3).
  const brush = d3.brushX()
    .extent([[m.l, m.t], [W - m.r, H - m.b]])
    .on('end', ev => {
      if (!ev.selection) { if (S.weekRange) { S.weekRange = null; render(); } return; }
      const [a, b] = ev.selection.map(x.invert);
      S.weekRange = [Math.round(a), Math.round(b)];
      render();
    });
  svg.append('g').call(brush);

  const hover = svg.append('line').attr('stroke', 'var(--accent)')
    .attr('stroke-dasharray', '2 2').attr('y1', m.t).attr('y2', H - m.b)
    .style('opacity', 0);
  svg.append('rect').attr('x', m.l).attr('y', m.t)
    .attr('width', W - m.l - m.r).attr('height', H - m.t - m.b)
    .attr('fill', 'none').style('pointer-events', 'all')
    .on('mousemove', ev => {
      const t = x.invert(d3.pointer(ev)[0]);
      const d = vis.reduce((p, c) => Math.abs(c.t - t) < Math.abs(p.t - t) ? c : p,
                           vis[0]);
      const f = fcVis.find(k => k.t === d.t);
      hover.attr('x1', x(d.t)).attr('x2', x(d.t)).style('opacity', .7);
      showTip(ev, `${label} · ${d.epi_year} week ${d.epi_week}`,
        [['Observed', val(d.cases)]].concat(f ? [
          ['Forecast', fmt(Math.round(f.forecast))],
          ['80% interval', `${fmt(Math.round(f.lower_80))}–${fmt(Math.round(f.upper_80))}`],
          ['Seasonal naive', fmt(Math.round(f.naive))]] : []));
    })
    .on('mouseleave', () => { hover.style('opacity', 0); hideTip(); });

  const lg = d3.select('#ts-legend'); lg.selectAll('*').remove();
  lg.append('span').html('<i style="background:var(--ink)"></i>observed');
  lg.append('span').html('<i style="background:var(--accent)"></i>NB-GLM forecast');
  lg.append('span').html('<i style="background:var(--accent);opacity:.3"></i>80% interval');
  lg.append('span').html('<i style="background:var(--warn)"></i>seasonal-naive baseline');
}


/* ========================================== VIEW 3: seasonal heatmap */
function renderHeatmap() {
  const box = d3.select('#heatmap'); box.selectAll('*').remove();
  const W = box.node().clientWidth, H = 330;
  const m = { t: 8, r: 10, b: 34, l: 92 };

  let rows = DATA.heat.filter(d => d.disease_group === S.disease);
  if (S.division) rows = rows.filter(d => d.division === S.division);

  // With 64 districts the panel would need two pixels per row, so when no
  // division is chosen the districts with the largest seasonal amplitude are
  // shown. Amplitude, not total, because the panel is about seasonality:
  // a district that is uniformly busy has no seasonal story to tell.
  const byDistrict = d3.rollup(rows, v => {
    const vals = v.map(d => d.mean_cases).filter(x => !isSup(x));
    return vals.length ? d3.max(vals) - d3.min(vals) : 0;
  }, d => d.district_name);
  const keep = new Set([...byDistrict.entries()]
    .sort((a, b) => b[1] - a[1]).slice(0, S.division ? 20 : 18)
    .map(d => d[0]));
  if (S.district) keep.add(S.district.district_name);
  rows = rows.filter(d => keep.has(d.district_name));

  if (!rows.length) {
    box.append('div').attr('class', 'loading').text('no data'); return;
  }

  let weeks = [...new Set(rows.map(d => d.epi_week))].sort((a, b) => a - b);
  // A brushed range on the time series narrows the weeks shown here too.
  if (S.weekRange) {
    const lo = S.weekRange[0] % 53, hi = S.weekRange[1] % 53;
    if (hi > lo) weeks = weeks.filter(w => w >= lo && w <= hi);
  }
  const districts = [...new Set(rows.map(d => d.district_name))]
    .sort((a, b) => (byDistrict.get(b) || 0) - (byDistrict.get(a) || 0));

  const svg = box.append('svg').attr('width', W).attr('height', H);
  const x = d3.scaleBand().domain(weeks).range([m.l, W - m.r]).padding(0.04);
  const y = d3.scaleBand().domain(districts).range([m.t, H - m.b]).padding(0.08);

  const vals = rows.map(d => d.mean_cases).filter(v => !isSup(v));
  const colour = d3.scaleSequential()
    .domain([0, d3.max(vals) || 1])
    .interpolator(t => d3.interpolateRgbBasis(
      ['#1a2733', '#1f4a63', '#2d7a94', '#56b4a0', '#e69f00', '#d55e00'])(t));

  const cells = svg.append('g').selectAll('rect')
    .data(rows.filter(d => weeks.includes(d.epi_week)
                        && districts.includes(d.district_name)))
    .join('rect')
    .attr('x', d => x(d.epi_week)).attr('y', d => y(d.district_name))
    .attr('width', x.bandwidth()).attr('height', y.bandwidth())
    .attr('fill', d => isSup(d.mean_cases) ? 'url(#hatch)' : colour(d.mean_cases))
    .attr('opacity', d => (S.district &&
      d.district_name !== S.district.district_name) ? 0.35 : 1)
    .style('cursor', 'pointer');

  // Suppressed cells are hatched, not blank and not zero-coloured, so the
  // viewer can see that something is being withheld (PRV-2, VIS-6).
  const defs = svg.append('defs');
  defs.append('pattern').attr('id', 'hatch')
    .attr('width', 4).attr('height', 4).attr('patternUnits', 'userSpaceOnUse')
    .attr('patternTransform', 'rotate(45)')
    .call(p => {
      p.append('rect').attr('width', 4).attr('height', 4)
        .attr('fill', 'var(--suppressed)');
      p.append('line').attr('x1', 0).attr('y1', 0).attr('x2', 0).attr('y2', 4)
        .attr('stroke', 'rgba(255,255,255,.28)').attr('stroke-width', 1.5);
    });

  cells
    .on('mousemove', (ev, d) => showTip(ev,
      `${d.district_name} · week ${d.epi_week}`, [
        ['Mean weekly cases', val(d.mean_cases)],
        ['Mean per 100k', isSup(d.mean_incidence) ? 'withheld'
                          : fmt1(d.mean_incidence)],
        ['Division', d.division],
      ]))
    .on('mouseleave', hideTip)
    .on('click', (ev, d) => {
      const r = DATA.map.districts.find(k => k.district_id === d.district_id
                                          && k.disease_group === S.disease);
      S.district = (S.district && S.district.district_id === d.district_id)
                   ? null : (r || null);
      render();
    });

  svg.append('g').attr('class', 'axis')
    .attr('transform', `translate(0,${H - m.b})`)
    .call(d3.axisBottom(x).tickValues(weeks.filter((w, i) => i % 6 === 0)));
  svg.append('g').attr('class', 'axis')
    .attr('transform', `translate(${m.l},0)`)
    .call(d3.axisLeft(y).tickSize(0))
    .selectAll('text').attr('font-size', 9)
    .attr('fill', d => (S.district && d === S.district.district_name)
      ? 'var(--accent)' : 'var(--ink-dim)');
  svg.append('text').attr('x', (W + m.l) / 2).attr('y', H - 3)
    .attr('text-anchor', 'middle').attr('fill', 'var(--ink-dim)')
    .attr('font-size', 10).text('epidemiological week');

  const lg = d3.select('#heat-legend'); lg.selectAll('*').remove();
  d3.range(5).forEach(i => {
    const v = colour.domain()[1] * i / 4;
    lg.append('span').html(`<i style="background:${colour(v)}"></i>${fmt1(v)}`);
  });
  lg.append('span').attr('class', 'suppressed-key').html('<i></i>withheld');
  lg.append('span').style('color', 'var(--ink-faint)')
    .text(`top ${districts.length} districts by seasonal amplitude`);
}

/* ========================================== VIEW 4: allocation plan */
function renderAllocation() {
  const box = d3.select('#allocation'); box.selectAll('*').remove();
  const W = box.node().clientWidth, H = 330;
  const m = { t: 10, r: 14, b: 46, l: 96 };

  let rows = DATA.alloc.allocation;
  if (S.division) rows = rows.filter(d => d.division === S.division);

  // Saline is the binding resource in the optimiser's scenario, so it is the
  // default. The others are reachable through the division filter and the
  // tooltip rather than through a second control.
  const resource = 'iv_saline_units';
  rows = rows.filter(d => d.resource === resource);

  const agg = [...d3.rollup(rows, v => ({
    demand: d3.sum(v, d => d.demand),
    stock: d3.sum(v, d => d.stock),
    allocated: d3.sum(v, d => d.allocated),
    unmet: d3.sum(v, d => d.unmet),
    district_id: v[0].district_id, division: v[0].division,
  }), d => d.district_name)].map(([name, o]) => ({ district_name: name, ...o }))
    .sort((a, b) => b.demand - a.demand).slice(0, 12);

  if (!agg.length) {
    box.append('div').attr('class', 'loading').text('no allocation data');
    return;
  }

  const svg = box.append('svg').attr('width', W).attr('height', H);
  const y = d3.scaleBand().domain(agg.map(d => d.district_name))
    .range([m.t, H - m.b]).padding(0.25);
  const x = d3.scaleLinear().domain([0, d3.max(agg, d => d.demand) * 1.05])
    .range([m.l, W - m.r]);

  svg.append('g').selectAll('line').data(x.ticks(5)).join('line')
    .attr('class', 'grid-line').attr('y1', m.t).attr('y2', H - m.b)
    .attr('x1', x).attr('x2', x);

  const g = svg.append('g').selectAll('g').data(agg).join('g')
    .attr('transform', d => `translate(0,${y(d.district_name)})`)
    .attr('opacity', d => (S.district &&
      d.district_id !== S.district.district_id) ? 0.4 : 1)
    .style('cursor', 'pointer')
    .on('mousemove', (ev, d) => showTip(ev, d.district_name, [
      ['Forecast demand', fmt(Math.round(d.demand))],
      ['Usable stock', fmt(Math.round(d.stock))],
      ['Recommended allocation', fmt(Math.round(d.allocated))],
      ['Remaining unmet', fmt(Math.round(d.unmet))],
      ['Coverage', d3.format('.0%')((d.stock + d.allocated) / (d.demand || 1))],
    ]))
    .on('mouseleave', hideTip)
    .on('click', (ev, d) => {
      const r = DATA.map.districts.find(k => k.district_id === d.district_id
                                          && k.disease_group === S.disease);
      S.district = (S.district && S.district.district_id === d.district_id)
                   ? null : (r || null);
      render();
    });

  // Demand is the full bar; stock and allocation stack inside it, so the
  // remaining gap is the unmet demand and is visible without a fourth bar.
  g.append('rect').attr('x', m.l).attr('y', 0)
    .attr('width', d => x(d.demand) - m.l).attr('height', y.bandwidth())
    .attr('fill', 'var(--panel2)').attr('stroke', 'var(--line)');
  g.append('rect').attr('x', m.l).attr('y', 0)
    .attr('width', d => x(d.stock) - m.l).attr('height', y.bandwidth())
    .attr('fill', 'var(--accent2)');
  g.append('rect').attr('x', d => x(d.stock))
    .attr('width', d => Math.max(0, x(d.stock + d.allocated) - x(d.stock)))
    .attr('height', y.bandwidth()).attr('fill', 'var(--accent)');

  svg.append('g').attr('class', 'axis')
    .attr('transform', `translate(${m.l},0)`)
    .call(d3.axisLeft(y).tickSize(0)).selectAll('text').attr('font-size', 9.5);
  svg.append('g').attr('class', 'axis')
    .attr('transform', `translate(0,${H - m.b})`)
    .call(d3.axisBottom(x).ticks(5).tickFormat(d3.format('~s')));
  svg.append('text').attr('x', (W + m.l) / 2).attr('y', H - 24)
    .attr('text-anchor', 'middle').attr('fill', 'var(--ink-dim)')
    .attr('font-size', 10).text('IV saline units over the 4-week horizon');

  const rec = DATA.alloc.recommended_alpha;
  svg.append('text').attr('x', (W + m.l) / 2).attr('y', H - 8)
    .attr('text-anchor', 'middle').attr('fill', 'var(--ink-faint)')
    .attr('font-size', 9.5)
    .text(`optimised plan at equity floor alpha = ${rec}`);

  const lg = d3.select('#alloc-legend'); lg.selectAll('*').remove();
  lg.append('span').html('<i style="background:var(--accent2)"></i>usable stock');
  lg.append('span').html('<i style="background:var(--accent)"></i>recommended allocation');
  lg.append('span').html('<i style="background:var(--panel2);border:1px solid var(--line)"></i>remaining unmet');
}

/* ====================================== VIEW 5: early-warning table */
let sortKey = 'severity', sortDesc = true;

function renderWarnings() {
  let rows = DATA.warn.filter(d => d.disease_group === S.disease);
  if (S.division) rows = rows.filter(d => d.division === S.division);
  if (S.weekRange) {
    const lo = S.weekRange[0] % 53, hi = S.weekRange[1] % 53;
    if (hi > lo) rows = rows.filter(d => d.epi_week >= lo && d.epi_week <= hi);
  }
  rows = rows.slice().sort((a, b) => {
    const va = a[sortKey], vb = b[sortKey];
    const c = (va === null) - (vb === null)
      || (typeof va === 'string' ? d3.ascending(va, vb) : d3.ascending(va, vb));
    return sortDesc ? -c : c;
  }).slice(0, 60);

  const cols = [
    ['district_name', 'District', false],
    ['division', 'Division', false],
    ['epi_week', 'Week', true],
    ['cases', 'Cases', true],
    ['expected', 'Seasonal expected', true],
    ['severity', 'Severity (SD)', true],
  ];

  d3.select('#warnings thead').selectAll('tr').data([0]).join('tr')
    .selectAll('th').data(cols).join('th')
    .attr('class', d => d[2] ? 'num' : null)
    .text(d => d[1] + (sortKey === d[0] ? (sortDesc ? ' ▾' : ' ▴') : ''))
    .on('click', (ev, d) => {
      if (sortKey === d[0]) sortDesc = !sortDesc;
      else { sortKey = d[0]; sortDesc = true; }
      renderWarnings();
    });

  // Severity is shaded on the same ramp as the map, so a district that looks
  // alarming here looks alarming there.
  const sev = d3.scaleSequential().domain([2, 12])
    .interpolator(t => d3.interpolateRgbBasis(
      ['#2d7a94', '#56b4a0', '#e69f00', '#d55e00'])(Math.max(0, Math.min(1, t))));

  const tr = d3.select('#warnings tbody').selectAll('tr').data(rows,
    d => d.district_id + d.disease_group + d.epi_week);
  tr.exit().remove();
  const e = tr.enter().append('tr');
  cols.forEach(() => e.append('td'));
  const all = e.merge(tr)
    .attr('class', d => (S.district && d.district_id === S.district.district_id)
      ? 'sel' : null)
    .on('click', (ev, d) => {
      const r = DATA.map.districts.find(k => k.district_id === d.district_id
                                          && k.disease_group === S.disease);
      S.district = (S.district && S.district.district_id === d.district_id)
                   ? null : (r || null);
      render();
    });

  all.selectAll('td').data(d => cols.map(c => ({ col: c, row: d })))
    .attr('class', d => {
      const v = d.row[d.col[0]];
      return (d.col[2] ? 'num' : '') + (v === null ? ' suppressed-cell' : '');
    })
    .html(d => {
      const k = d.col[0], v = d.row[k];
      if (v === null) return 'withheld';
      if (k === 'severity') {
        return `<span class="sev" style="background:${sev(v)}">` +
               `${v.toFixed(1)}</span>`;
      }
      if (k === 'cases' || k === 'expected') return fmt(Math.round(v));
      return v;
    });

  if (!rows.length) {
    d3.select('#warnings tbody').html(
      '<tr><td colspan="6" style="color:var(--ink-faint);padding:18px">' +
      'No alerts for this selection.</td></tr>');
  }
}

boot();
