function showTab(event, tab) {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.getElementById('tab-' + tab).classList.add('active');

    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    event.target.classList.add('active');
}

let currentAirline = "all";
let refreshInterval = 5000;
let intervalId;
let trendingChart, avgScoreChart;

function updateAirlineSelection() {
    currentAirline = document.getElementById("airlineFilter").value;
    loadRealtimeCharts();
}

function loadRealtimeCharts() {
    loadTrendingChart();
    loadSentimentScoreChart();
    loadComments();
}

function loadTrendingChart() {
    const url = currentAirline === "all"
        ? "/api/stream_trending_totals_realtime"
        : `/api/stream_trending_by_airline_realtime?airline=${encodeURIComponent(currentAirline)}`;

    fetch(url)
        .then(r => r.json())
        .then(data => {
            if (!data.labels || !data.datasets) return;

            const series = data.datasets.map(ds => ({
                name: ds.label,
                data: ds.data,
                color: ds.backgroundColor
            }));

            if (!trendingChart) {
                trendingChart = Highcharts.chart('chart_trending', {
                    chart: { type: 'line' },
                    title: { text: 'Trending cảm xúc (Realtime)' },
                    xAxis: { categories: data.labels, title: { text: 'Thời gian' } },
                    yAxis: { title: { text: 'Số lượng comment' }, min: 0 },
                    tooltip: { shared: true },
                    plotOptions: { line: { dataLabels: { enabled: true } } },
                    series: series
                });
            } else {
                trendingChart.update({
                    xAxis: { categories: data.labels },
                    series: series
                }, true, true);
            }
        })
        .catch(err => console.error("loadTrendingChart error:", err));
}

// Gắn sự kiện dropdown
document.addEventListener('DOMContentLoaded', () => {
    const select = document.getElementById('airlineTrendingSelect');
    select.addEventListener('change', () => {
        currentAirline = select.value;
        loadTrendingChart();
    });
});

function loadSentimentScoreChart() {
    fetch("/api/stream_sentiment_score_realtime")
        .then(r => r.json())
        .then(data => {
            if (!data.datasets || data.datasets.length === 0) return;

            const colors = ['#0077c2', '#e60023', '#ffc107', '#28a745', '#6f42c1', '#fd7e14'];
            const series = data.datasets.map((ds, idx) => ({
                name: ds.label,
                data: ds.data,
                color: colors[idx % colors.length]
            }));

            Highcharts.chart('chart_sentiment_score', {
                chart: { type: 'line' },
                title: { text: 'Điểm cảm xúc trung bình theo hãng' },
                xAxis: { categories: data.labels, title: { text: 'Thời gian' } },
                yAxis: { min: 0, max: 1, title: { text: 'Điểm trung bình' } },
                tooltip: { shared: true, valueDecimals: 2 },
                plotOptions: {
                    line: { dataLabels: { enabled: false }, enableMouseTracking: true }
                },
                series: series
            });
        })
        .catch(err => console.error("loadSentimentScoreChart error:", err));
}


function loadComments() {
    const url = currentAirline === "all"
        ? "/api/stream_latest_comments_realtime?limit=10"
        : `/api/stream_latest_comments_realtime?airline=${currentAirline}&limit=10`;

    fetch(url)
        .then(r => r.json())
        .then(rows => {
            const box = document.getElementById("comments_box_content");
            box.innerHTML = rows.map(c => `
                <div style="margin-bottom:8px">
                    <strong>${c.airline}</strong> – <em>${c.sentiment}</em><br>
                    ${c.text}<br>
                    <small>${c.time}</small>
                </div>`).join('');
        });
}

function startRealtimeUpdates() {
    loadRealtimeCharts();
    if (intervalId) clearInterval(intervalId);
    intervalId = setInterval(loadRealtimeCharts, refreshInterval);
}

/* ========================== HISTORY ========================== */
/* ========================== HISTORY CHARTS ========================== */

// 1️⃣ Pie chart tổng sentiment
function loadDonut() {
    fetch("/api/batch_totals")
        .then(r => r.json())
        .then(totalData => {

            fetch("/api/batch_grouped")
                .then(r => r.json())
                .then(groupedData => {

                    const drilldownSeries = [];

                    totalData.labels.forEach((sentimentLabel, idx) => {
                        const dataset = groupedData.datasets.find(ds => ds.label.toLowerCase() === sentimentLabel.toLowerCase());
                        const dataPerAirline = [];

                        if (dataset) {
                            dataset.data.forEach((val, i) => {
                                if (val !== null && val > 0) {
                                    dataPerAirline.push({
                                        name: groupedData.labels[i],
                                        y: val
                                    });
                                }
                            });
                        }

                        drilldownSeries.push({
                            id: sentimentLabel,
                            name: `Tỷ lệ ${sentimentLabel} theo hãng`,
                            data: dataPerAirline
                        });
                    });

                    // Chuẩn bị series chính cho donut
                    const seriesData = totalData.labels.map((label, i) => ({
                        name: label,
                        y: totalData.datasets[0].data[i] || 0,
                        color: totalData.datasets[0].backgroundColor[i],
                        drilldown: label
                    }));

                    // Vẽ chart
                    Highcharts.chart('chart_donut', {
                        chart: { type: 'pie' },
                        title: { text: 'Tổng Sentiment' },
                        tooltip: { pointFormat: '{series.name}: <b>{point.y}</b>' },
                        accessibility: { point: { valueSuffix: '' } },
                        plotOptions: {
                            pie: {
                                allowPointSelect: true,
                                cursor: 'pointer',
                                dataLabels: { enabled: true, format: '{point.name}: {point.y}' }
                            }
                        },
                        series: [{
                            name: totalData.datasets[0].label,
                            colorByPoint: true,
                            data: seriesData
                        }],
                        drilldown: { series: drilldownSeries }
                    });

                })
                .catch(err => console.error("Lỗi khi load groupedData:", err));

        })
        .catch(err => console.error("Lỗi khi load totalData:", err));
}


// Bar chart tổng lý do negative

function loadNegativeReason(airline = "All") {
    let url = airline === "All" ? "/api/batch_negativereason_total" : "/api/batch_negativereason_by_airline";

    fetch(url)
        .then(r => r.json())
        .then(data => {
            let categories = [];
            let counts = [];
            let chartTitle = "Tổng Negative Reasons";

            if (airline === "All") {
                categories = data.labels;
                counts = data.datasets[0].data;
            } else {
                if (!data[airline]) {
                    categories = [];
                    counts = [];
                } else {
                    categories = data[airline].labels;
                    counts = data[airline].data;
                    chartTitle = `Negative Reasons - ${airline}`;
                }
            }

            Highcharts.chart('chart_negative_reason_total', {
                chart: { type: 'column' },
                title: { text: chartTitle },
                xAxis: { categories: categories, title: { text: 'Reason' } },
                yAxis: { min: 0, title: { text: 'Count' } },
                tooltip: { pointFormat: '{series.name}: <b>{point.y}</b>' },
                series: [{
                    name: airline === "All" ? "Tổng Negative Reasons" : airline,
                    data: counts,
                    color: 'rgba(255, 99, 132, 0.6)'
                }]
            });
        })
        .catch(err => console.error("loadNegativeReason error:", err));
}

// Gắn event dropdown
document.addEventListener('DOMContentLoaded', function() {
    const select = document.getElementById('airlineSelect');
    select.addEventListener('change', function() {
        loadNegativeReason(this.value);
    });

    // Load mặc định
    loadNegativeReason("All");
});


// Bar chart điểm trung bình cảm xúc theo hãng
function loadSentimentChart() {
    fetch('/api/airline_sentiment_avg')
        .then(response => response.json())
        .then(data => {
            Highcharts.chart('sentiment-chart', {
                chart: {
                    type: 'column'
                },
                xAxis: {
                    categories: data.labels,
                    title: {
                        text: 'Hãng hàng không'
                    }
                },
                yAxis: {
                    min: 0,
                    max: 1,
                    title: {
                        text: 'Điểm cảm xúc trung bình'
                    }
                },
                series: [{
                    name: data.datasets[0].label,
                    data: data.datasets[0].data,
                    color: data.datasets[0].backgroundColor
                }],
                tooltip: {
                    pointFormat: '<b>{point.y:.2f}</b>'
                }
            });
        })
        .catch(err => console.error('Lỗi khi lấy dữ liệu:', err));
}

// Gọi hàm khi trang đã load xong
document.addEventListener('DOMContentLoaded', loadSentimentChart);

function loadHistoryCharts() {
    loadDonut();
    loadNegativeReasonTotal();
    loadSentimentChart();
}

/* ========================== ONLOAD ========================== */
window.onload = () => {
    startRealtimeUpdates();
    loadHistoryCharts();
};
