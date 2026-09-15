import 'package:flutter/material.dart';
import 'package:http/http.dart' as http;

import 'dart:convert';

import 'package:fl_chart/fl_chart.dart';

void main() {
  runApp(const TradingBacktestApp());
}

class TradingBacktestApp extends StatelessWidget {
  const TradingBacktestApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      debugShowCheckedModeBanner: false,
      title: 'ND Backtest',
      theme: ThemeData.dark().copyWith(
        scaffoldBackgroundColor: const Color(0xFF101216),
        primaryColor: Colors.cyanAccent,
      ),
      home: const BacktestScreen(),
    );
  }
}

class BacktestScreen extends StatefulWidget {
  const BacktestScreen({super.key});

  @override
  State<BacktestScreen> createState() => _BacktestScreenState();
}

class _BacktestScreenState extends State<BacktestScreen> {
  String _mode = 'indicator'; // 'indicator' किंवा 'custom_code'
  String _dataSource = 'yfinance';
  String _selectedSymbol = '^NSEI';
  String _timeframe = '15m';

  final TextEditingController _capitalController = TextEditingController(
    text: '100000',
  );
  final TextEditingController _riskPctController = TextEditingController(
    text: '1.0',
  );
  final TextEditingController _rrController = TextEditingController(
    text: '2.0',
  );
  final TextEditingController _fastEmaController = TextEditingController(
    text: '9',
  );
  final TextEditingController _slowEmaController = TextEditingController(
    text: '21',
  );

  // डीफॉल्ट कस्टम स्ट्रॅटेजी कोड (No Lookahead Bias)
  final TextEditingController _codeController = TextEditingController(
    text: '''# Custom Python Strategy
# Available variables: df, np, pd, ta
# df columns: ['open', 'high', 'low', 'close', 'volume']
# Set df['signal']: 1 = Buy, -1 = Sell, 0 = Hold

df['sma_20'] = df['close'].rolling(20).mean()
df['sma_50'] = df['close'].rolling(50).mean()

df['signal'] = 0
df.loc[df['sma_20'] > df['sma_50'], 'signal'] = 1
df.loc[df['sma_20'] < df['sma_50'], 'signal'] = -1
''',
  );

  List<dynamic> _symbols = [];
  bool _loading = false;
  Map<String, dynamic>? _results;
  String _errorMessage = '';

  @override
  void initState() {
    super.initState();
    _fetchSymbols();
  }

  Future<void> _fetchSymbols() async {
    try {
      final res = await http.get(Uri.parse('http://127.0.0.1:8000/symbols'));
      if (res.statusCode == 200) {
        setState(() {
          _symbols = jsonDecode(res.body);
          if (_symbols.isNotEmpty) {
            _selectedSymbol = _symbols[0]['symbol'];
            _dataSource = _symbols[0]['source'];
          }
        });
      }
    } catch (_) {}
  }

  Future<void> _runBacktest() async {
    setState(() {
      _loading = true;
      _errorMessage = '';
      _results = null;
    });

    final payload = {
      "mode": _mode,
      "data_source": _dataSource,
      "symbol": _selectedSymbol,
      "timeframe": _timeframe,
      "capital": double.tryParse(_capitalController.text) ?? 100000.0,
      "risk_pct": double.tryParse(_riskPctController.text) ?? 1.0,
      "risk_reward": double.tryParse(_rrController.text) ?? 2.0,
      "fast_ema": int.tryParse(_fastEmaController.text) ?? 9,
      "slow_ema": int.tryParse(_slowEmaController.text) ?? 21,
      "rsi_period": 14,
      "custom_strategy_code": _mode == "custom_code"
          ? _codeController.text
          : null,
    };

    try {
      final response = await http.post(
        Uri.parse('http://127.0.0.1:8000/run-backtest'),
        headers: {"Content-Type": "application/json"},
        body: jsonEncode(payload),
      );

      final data = jsonDecode(response.body);
      if (response.statusCode == 200 && data['status'] == 'success') {
        setState(() => _results = data);
      } else {
        setState(() => _errorMessage = data['message'] ?? 'त्रुटी आढळली.');
      }
    } catch (e) {
      setState(() => _errorMessage = "सर्व्हर कनेक्शन अयशस्वी: $e");
    } finally {
      setState(() => _loading = false);
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('ND Backtest Engine'),
        backgroundColor: const Color(0xFF1B1E24),
      ),
      body: SingleChildScrollView(
        padding: const EdgeInsets.all(16.0),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            // Mode Selector
            SegmentedButton<String>(
              segments: const [
                ButtonSegment(
                  value: 'indicator',
                  label: Text('इंडिकेटर मोड (No Code)'),
                ),
                ButtonSegment(
                  value: 'custom_code',
                  label: Text('कस्टम पायथन कोड'),
                ),
              ],
              selected: {_mode},
              onSelectionChanged: (set) => setState(() => _mode = set.first),
            ),
            const SizedBox(height: 16),

            // Market / Asset Selector
            Card(
              color: const Color(0xFF1B1E24),
              child: Padding(
                padding: const EdgeInsets.all(16.0),
                child: Column(
                  children: [
                    Row(
                      children: [
                        Expanded(
                          child: DropdownButtonFormField<String>(
                            value:
                                _symbols.any(
                                  (e) => e['symbol'] == _selectedSymbol,
                                )
                                ? _selectedSymbol
                                : null,
                            decoration: const InputDecoration(
                              labelText: 'सिम्बॉल (Stock / Crypto / Index)',
                            ),
                            items: _symbols.map<DropdownMenuItem<String>>((s) {
                              return DropdownMenuItem(
                                value: s['symbol'],
                                child: Text("${s['name']} (${s['category']})"),
                              );
                            }).toList(),
                            onChanged: (val) {
                              if (val != null) {
                                setState(() {
                                  _selectedSymbol = val;
                                  final sObj = _symbols.firstWhere(
                                    (e) => e['symbol'] == val,
                                  );
                                  _dataSource = sObj['source'];
                                });
                              }
                            },
                          ),
                        ),
                        const SizedBox(width: 12),
                        SizedBox(
                          width: 110,
                          child: DropdownButtonFormField<String>(
                            value: _timeframe,
                            decoration: const InputDecoration(
                              labelText: 'Timeframe',
                            ),
                            items: const [
                              DropdownMenuItem(value: '5m', child: Text('5m')),
                              DropdownMenuItem(
                                value: '15m',
                                child: Text('15m'),
                              ),
                              DropdownMenuItem(value: '1h', child: Text('1h')),
                              DropdownMenuItem(value: '1d', child: Text('1d')),
                            ],
                            onChanged: (val) =>
                                setState(() => _timeframe = val!),
                          ),
                        ),
                      ],
                    ),
                    const SizedBox(height: 12),
                    Row(
                      children: [
                        Expanded(
                          child: TextField(
                            controller: _capitalController,
                            keyboardType: TextInputType.number,
                            decoration: const InputDecoration(
                              labelText: 'कॅपिटल (₹ / \$)',
                            ),
                          ),
                        ),
                        const SizedBox(width: 12),
                        Expanded(
                          child: TextField(
                            controller: _riskPctController,
                            keyboardType: TextInputType.number,
                            decoration: const InputDecoration(
                              labelText: 'Risk % Per Trade',
                            ),
                          ),
                        ),
                        const SizedBox(width: 12),
                        Expanded(
                          child: TextField(
                            controller: _rrController,
                            keyboardType: TextInputType.number,
                            decoration: const InputDecoration(
                              labelText: 'Risk-Reward Ratio',
                            ),
                          ),
                        ),
                      ],
                    ),
                  ],
                ),
              ),
            ),
            const SizedBox(height: 16),

            // Dynamic Config View (Code Editor or Indicators)
            if (_mode == 'indicator')
              Card(
                color: const Color(0xFF1B1E24),
                child: Padding(
                  padding: const EdgeInsets.all(16.0),
                  child: Row(
                    children: [
                      Expanded(
                        child: TextField(
                          controller: _fastEmaController,
                          decoration: const InputDecoration(
                            labelText: 'Fast EMA',
                          ),
                        ),
                      ),
                      const SizedBox(width: 12),
                      Expanded(
                        child: TextField(
                          controller: _slowEmaController,
                          decoration: const InputDecoration(
                            labelText: 'Slow EMA',
                          ),
                        ),
                      ),
                    ],
                  ),
                ),
              )
            else
              Card(
                color: const Color(0xFF1B1E24),
                child: Padding(
                  padding: const EdgeInsets.all(16.0),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      const Text(
                        "तुमचा Python कोड लिहा (Vectorized Pandas logic):",
                        style: TextStyle(fontWeight: FontWeight.bold),
                      ),
                      const SizedBox(height: 8),
                      TextField(
                        controller: _codeController,
                        maxLines: 8,
                        style: const TextStyle(
                          fontFamily: 'monospace',
                          fontSize: 13,
                        ),
                        decoration: const InputDecoration(
                          border: OutlineInputBorder(),
                          filled: true,
                          fillColor: Color(0xFF0F1115),
                        ),
                      ),
                    ],
                  ),
                ),
              ),

            const SizedBox(height: 16),
            ElevatedButton(
              style: ElevatedButton.styleFrom(
                minimumSize: const Size(double.infinity, 50),
                backgroundColor: Colors.cyanAccent,
                foregroundColor: Colors.black,
              ),
              onPressed: _loading ? null : _runBacktest,
              child: _loading
                  ? const CircularProgressIndicator(color: Colors.black)
                  : const Text(
                      'बॅकटेस्ट चालवा (Run Quantitative Test)',
                      style: TextStyle(
                        fontSize: 16,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
            ),

            if (_errorMessage.isNotEmpty)
              Padding(
                padding: const EdgeInsets.symmetric(vertical: 12.0),
                child: Text(
                  _errorMessage,
                  style: const TextStyle(color: Colors.redAccent),
                ),
              ),

            // Performance Results
            if (_results != null) ...[
              const SizedBox(height: 24),
              _buildMetricsGrid(),
              const SizedBox(height: 24),
              _buildEquityCurve(),
              const SizedBox(height: 24),
              _buildTradeLogs(),
            ],
          ],
        ),
      ),
    );
  }

  Widget _buildMetricsGrid() {
    return Card(
      color: const Color(0xFF1B1E24),
      child: Padding(
        padding: const EdgeInsets.all(16.0),
        child: Column(
          children: [
            Row(
              mainAxisAlignment: MainAxisAlignment.spaceBetween,
              children: [
                _metricTile(
                  'निव्वळ नफा (Net PnL)',
                  "${_results!['net_pnl']}",
                  _results!['net_pnl'] >= 0
                      ? Colors.greenAccent
                      : Colors.redAccent,
                ),
                _metricTile(
                  'ROI %',
                  "${_results!['roi_pct']}%",
                  _results!['roi_pct'] >= 0
                      ? Colors.greenAccent
                      : Colors.redAccent,
                ),
                _metricTile(
                  'Win Rate',
                  "${_results!['win_rate_pct']}%",
                  Colors.cyanAccent,
                ),
                _metricTile(
                  'Max Drawdown',
                  "${_results!['max_drawdown_pct']}%",
                  Colors.orangeAccent,
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }

  Widget _metricTile(String label, String value, Color color) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(label, style: const TextStyle(color: Colors.grey, fontSize: 12)),
        const SizedBox(height: 4),
        Text(
          value,
          style: TextStyle(
            fontSize: 16,
            fontWeight: FontWeight.bold,
            color: color,
          ),
        ),
      ],
    );
  }

  Widget _buildEquityCurve() {
    final List equityList = _results!['equity_curve'] ?? [];
    List<FlSpot> spots = [];
    for (int i = 0; i < equityList.length; i++) {
      spots.add(
        FlSpot(i.toDouble(), (equityList[i]['equity'] as num).toDouble()),
      );
    }

    return Card(
      color: const Color(0xFF1B1E24),
      child: Padding(
        padding: const EdgeInsets.all(16.0),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            const Text(
              'Equity Curve (पोर्टफोलिओ वाढ)',
              style: TextStyle(fontSize: 16, fontWeight: FontWeight.bold),
            ),
            const SizedBox(height: 16),
            SizedBox(
              height: 220,
              child: spots.isEmpty
                  ? const Center(child: Text("डेटा उपलब्ध नाही"))
                  : LineChart(
                      LineChartData(
                        gridData: const FlGridData(show: false),
                        titlesData: const FlTitlesData(show: false),
                        borderData: FlBorderData(show: false),
                        lineBarsData: [
                          LineChartBarData(
                            spots: spots,
                            isCurved: true,
                            color: Colors.cyanAccent,
                            barWidth: 2,
                            dotData: const FlDotData(show: false),
                            belowBarData: BarAreaData(
                              show: true,
                              color: Colors.cyanAccent.withOpacity(0.15),
                            ),
                          ),
                        ],
                      ),
                    ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildTradeLogs() {
    final List logs = _results!['trade_logs'] ?? [];
    return Card(
      color: const Color(0xFF1B1E24),
      child: Padding(
        padding: const EdgeInsets.all(16.0),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            const Text(
              'Trade Logs (शेवटचे ट्रेड्स)',
              style: TextStyle(fontSize: 16, fontWeight: FontWeight.bold),
            ),
            const Divider(),
            if (logs.isEmpty)
              const Text("कोणतेही ट्रेड्स झाले नाहीत.")
            else
              ...logs.map((trade) {
                final isProfit = trade['pnl'] >= 0;
                return ListTile(
                  dense: true,
                  contentPadding: EdgeInsets.zero,
                  title: Text(
                    "${trade['type']} @ ${trade['entry_price']} ➔ ${trade['exit_price']}",
                  ),
                  subtitle: Text(
                    "Entry: ${trade['entry_time']} | Exit: ${trade['exit_time']}",
                  ),
                  trailing: Text(
                    "${isProfit ? '+' : ''}${trade['pnl']} (${trade['return_pct']}%)",
                    style: TextStyle(
                      color: isProfit ? Colors.greenAccent : Colors.redAccent,
                      fontWeight: FontWeight.bold,
                    ),
                  ),
                );
              }).toList(),
          ],
        ),
      ),
    );
  }
}
