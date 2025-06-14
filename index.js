// 必要なライブラリの読み込み
const express = require('express');
const { Pool } = require('pg');

const app = express();

// JSONボディの受信を可能にするミドルウェア
app.use(express.json());

// PostgreSQLとの接続設定（Render上の環境変数 DATABASE_URL を使用）
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false } // RenderではSSLが必要
});

// GASなどから送信されたJSONデータを受け取るエンドポイント
app.post('/api/orders/upload', async (req, res) => {
  const rows = req.body.data; // data配列としてJSONで送られてくる受注データ
  const client = await pool.connect();

  try {
    await client.query('BEGIN'); // トランザクション開始

    for (const row of rows) {
      // JSONオブジェクトから必要な項目を取り出す
      const {
        order_id, order_date, sales_dept, customer_name, customer_id,
        product_code, product_name, quantity, unit_price, total_price,
        currency, delivery_date, order_status, jpy_value
      } = row;

      // 注文ステータスが空でない（キャンセル・変更など）場合は、同じorder_idのデータを削除
      if (order_status && order_status.trim() !== "") {
        await client.query(`DELETE FROM orders WHERE order_id = $1`, [order_id]);
      }

      // 新規データとして追加（削除済みデータも含む）
      await client.query(`
        INSERT INTO orders (
          order_id, order_date, sales_dept, customer_name, customer_id,
          product_code, product_name, quantity, unit_price, total_price,
          currency, delivery_date, order_status, jpy_value
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
      `, [
        order_id, order_date, sales_dept, customer_name, customer_id,
        product_code, product_name, quantity, unit_price, total_price,
        currency, delivery_date, order_status, jpy_value
      ]);
    }

    await client.query('COMMIT'); // コミットしてDBに反映
    res.status(200).send('DB登録完了'); // 成功レスポンス
  } catch (err) {
    await client.query('ROLLBACK'); // エラー時はロールバック
    console.error(err);
    res.status(500).send('DBエラー');
  } finally {
    client.release(); // DB接続を解放
  }
});

// csv-stringifyをインポート
const { stringify } = require('csv-stringify');

// データベース接続テスト (オプション: サーバー起動時にログに出力)
pool.connect((err, client, release) => {
  if (err) {
    return console.error('Error acquiring client', err.stack);
  }
  console.log('Successfully connected to PostgreSQL database!');
  release();
});

// 今年度の受注データをCSVとしてエクスポートするAPIエンドポイント
app.get('/api/orders/export-current-fiscal-year', async (req, res) => {
  try {
    const today = new Date();
    const currentYear = today.getFullYear();
    const currentMonth = today.getMonth() + 1; // getMonth()は0-indexedのため+1

    // データベースクエリとログ用の会計年度文字列
    let fiscalYear;

    // 4月から新年度と仮定して会計年度を計算
    if (currentMonth >= 4) {
      fiscalYear = `${currentYear}年度`;
    } else {
      fiscalYear = `${currentYear - 1}年度`;
    }

    console.log(`Exporting data for fiscal year: ${fiscalYear}`);

    const query = `
      SELECT *
      FROM orders_view
      WHERE fiscal_year = $1;
    `;
    const result = await pool.query(query, [fiscalYear]); // fiscalYearを使用
    let records = result.rows;

    if (records.length === 0) {
      console.log(`No records found for fiscal year: ${fiscalYear}`);
      return res.status(404).send(`No order data found for fiscal year ${fiscalYear}.`);
    }

      // 日付列をYYYY-MM-DD形式の文字列に変換する
      const formattedRecords = records.map(record => {
      const newRecord = { ...record }; // 元のレコードを直接変更しないようにコピー

      // order_date 列を変換
      if (newRecord.order_date instanceof Date) {
        newRecord.order_date = newRecord.order_date.toISOString().split('T')[0];
      }
      // delivery_date 列を変換（もし存在し、Dateオブジェクトの場合）
      if (newRecord.delivery_date instanceof Date) {
        newRecord.delivery_date = newRecord.delivery_date.toISOString().split('T')[0];
      }
      // 他にも日付として扱いたい列があればここに追加
      // 例: if (newRecord.some_other_date_column instanceof Date) { ... }

      return newRecord;
    });

    records = formattedRecords; // フォーマット済みのレコードを使用する

    // records[0]が存在することが保証された後にcolumnsを定義
    const columns = Object.keys(records[0]);

    // stringifyのPromise化
    const csvString = await new Promise((resolve, reject) => {
      stringify(records, { header: true, columns: columns }, (err, resultCsv) => {
        if (err) {
          console.error('Error generating CSV string:', err);
          return reject(err);
        }
        resolve(resultCsv);
      });
    });

    // CSV文字列をGASに直接返すためのヘッダーを設定
    res.setHeader('Content-Type', 'text/csv; charset=utf-8'); // CSV形式であることを明示し、文字コードを指定

    res.status(200).send(csvString);
    console.log(`Successfully sent ${records.length} records for ${fiscalYear}.`); // ログメッセージを"sent"に変更

  } catch (err) {
    console.error('Error exporting order data or generating CSV:', err.stack);
    res.status(500).send('Error generating CSV data.');
  }
});


// テスト用エンドポイント（ブラウザでアクセスして動作確認可能）
app.get('/', (req, res) => {
  res.send('Order Import API (JSON)');
});

// サーバー起動（Renderでは環境変数PORTが設定されている）
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`サーバーがポート${PORT}で起動中`);
});
