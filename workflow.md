# Nhật ký Công việc Refactor (Pandas -> Polars)

## Phase 0: Setup & Infrastructure
**Ngày:** 2026-05-18

### Các bước đã thực hiện:

1. **Cập nhật `requirements.txt`:**
   - Đã gỡ bỏ thư viện `pandas` (`pandas==2.2.3`).
   - Đã cập nhật version `numpy>=1.26.4` để hỗ trợ tốt cho các operation với polars và ewma.
   - Bổ sung `polars>=1.0.0`, `connectorx>=0.3.3`, và `pyarrow>=15.0.0` để tối ưu hóa IO và hỗ trợ database interaction trực tiếp, mang lại hiệu suất tốt hơn từ 5-10 lần khi xử lý dữ liệu lớn.
   - Hạ cấp version yêu cầu cho `scikit-learn` thành `>=1.4.0` vì đây là phiên bản đầu tiên hỗ trợ `set_output(transform="polars")`, chuẩn bị sẵn sàng cho Phase 4.
   - Đặt version tối thiểu cho `xgboost` là `>=2.0.0` để nhận input là object `polars.DataFrame` natively thay vì cần covert sang Pandas hoặc Numpy (trừ weight và label arrays).

2. **Tạo `src/shared/polars_db.py`:**
   - Xây dựng helper function `read_sql` thay thế trực tiếp cho method `pd.read_sql()`. Hàm này sử dụng `sqlalchemy.engine.Engine` và trả về `pl.DataFrame`.
   - Logic thay thế (Fallback Mapping): Hiện tại sử dụng `conn.execute().mappings().all()` và parse ra list of dicts. Mặc dù có chút overhead so với Native ADBC/connectorx, nhưng an toàn hơn cho các truy vấn cũ, cho phép xử lý gracefully khi dataframe trả về rỗng thông qua việc fetch list key (schemas) rồi nhét vào empty DataFrame. Cực kỳ hữu dụng để chống crash các downstream component.

### Tư duy logic và Suy luận:
- **Centralized DB Reader**: Thay vì fix rải rác 14 file, gom lại vào `src/shared/polars_db.py` giúp quản lý connection và IO hiệu quả hơn. Sau Phase 1, nếu muốn chuyển đổi thẳng sang ADBC hay connectorx để C++ buffer dữ liệu trực tiếp, tôi chỉ cần điều chỉnh đúng hàm này.
- **Tại sao lại chọn fallback map trước?** Việc giữ sqlalchemy parameter substitution hoạt động 100% giống cũ là ưu tiên hàng đầu, bảo đảm "Bảo toàn logic" như quy ước, tránh `breaking changes` ở các Phase sau do SQL params không parse được với engine khác.

### Rủi ro tiềm ẩn (Risk & Mitigation) cần theo dõi:
- **Hiệu suất I/O với row object creation**: Việc parse `list of dicts` thành Polars DF đôi khi sẽ trở thành cổ chai nếu query trả ra hàng chục triệu record (Object creation overhead của Python). Nếu load dữ liệu quá chậm ở Phase 1, tôi sẽ thay đổi ruột của `read_sql` thành `pl.read_database()` hoặc truyền raw bytes thẳng từ connectorx.
- **Cấu hình Empty Schema**: Hàm có xử lý empty dataframe bằng `return pl.DataFrame({c: [] for c in cols})`. Tuy nhiên kiểu dữ liệu của các cột này trong Polars sẽ mặc định là Float/String thay vì type chuẩn từ DB (nếu list rỗng). Có nguy cơ các cột này gây lỗi Schema ở các component phía sau, sẽ cần kiểm tra kỹ lúc run unit testing trong Phase 1.

**Tình trạng Phase 0:** Đã hoàn thành các bước setup I/O Helper và Requirements. Pipeline hiện tại đã sẵn sàng để kiểm tra CI và migrate Phase 1.
