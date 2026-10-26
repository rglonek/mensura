const path = require('path');

module.exports = {
  mode: 'production',
  entry: './src/module.tsx',
  output: {
    path: path.resolve(__dirname, 'dist'),
    filename: 'module.js',
    libraryTarget: 'amd',
    clean: true,
  },
  externals: [
    '@grafana/data',
    '@grafana/ui',
    '@grafana/runtime',
    'react',
    'react-dom',
    function ({ request }, callback) {
      const prefix = 'grafana/';
      if (request && request.indexOf(prefix) === 0) {
        return callback(null, request);
      }
      callback();
    },
  ],
  resolve: { extensions: ['.ts', '.tsx', '.js'] },
  module: {
    rules: [{ test: /\.tsx?$/, use: 'ts-loader', exclude: /node_modules/ }],
  },
};
