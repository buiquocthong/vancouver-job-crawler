# Vancouver Job Crawler 🇨🇦

Tự động crawl Indeed + Glassdoor lấy jobs Finance / Data Science / C-Suite
tại khu vực **Vancouver BC** với lương **≥ $60k/năm** trong **3 ngày gần nhất**.

---

## Cài đặt

```bash
pip install python-jobspy pandas
```

## Chạy ngay (demo — không cần proxy)

```bash
python3 vancouver_job_crawler.py --demo
```

## Chạy thật (cần VPN/Proxy)

**Bước 1**: Mở `vancouver_job_crawler.py`, tìm dòng:
```python
PROXIES: list[str] = []
```
Điền proxy vào:
```python
PROXIES = ["user:password@proxy.host:8080"]
```

**Bước 2**: Chạy:
```bash
python3 vancouver_job_crawler.py
```

Kết quả: `vancouver_jobs_YYYY-MM-DD.csv`

---

## Proxy/VPN khuyến nghị

| Loại         | Gợi ý                           | Ghi chú                         |
|-------------|----------------------------------|----------------------------------|
| Residential | Smartproxy, Oxylabs, Bright Data | Tốt nhất, ít bị block nhất       |
| VPN local   | Cài VPN trên máy rồi chạy script | Miễn phí nhưng có thể chậm hơn  |
| Datacenter  | Webshare, ProxyEmpire            | Rẻ hơn, Indeed có thể block     |

**Lưu ý**: Indeed và Glassdoor chặn datacenter IP. Nếu chạy trên VPS/server,
bắt buộc phải có residential proxy.

---

## Cấu hình tùy chỉnh

Mở file `vancouver_job_crawler.py`, tìm phần **① CẤU HÌNH**:

| Biến                | Mặc định       | Ý nghĩa                        |
|--------------------|----------------|--------------------------------|
| `PROXIES`          | `[]`           | Danh sách proxy                |
| `DAYS_OLD`         | `3`            | Lấy N ngày gần nhất            |
| `MIN_ANNUAL`       | `60_000`       | Lương tối thiểu (CAD/năm)      |
| `MIN_HOURLY`       | `30.0`         | Lương tối thiểu (CAD/giờ)      |
| `RESULTS_PER_SEARCH`| `50`          | Số jobs mỗi lần tìm            |
| `SITES`            | `indeed, glassdoor` | Nguồn crawl              |

---

## Output CSV — Các cột

| Cột               | Mô tả                                      |
|------------------|--------------------------------------------|
| `title`          | Tên vị trí                                 |
| `company_name`   | Tên công ty                                |
| `location_str`   | Thành phố, BC, Canada                      |
| `salary_display` | VD: `CAD $90,000 – $115,000/yr`            |
| `apply_method`   | `Apply Now` hoặc `Apply on Company Site`   |
| `date_posted`    | Ngày đăng tuyển                            |
| `job_url`        | Link Indeed/Glassdoor                      |
| `job_url_direct` | Link ATS công ty (nếu Apply on Company Site)|
| `site`           | `indeed` hoặc `glassdoor`                  |
| `search_keyword` | Keyword đã dùng để tìm                     |

---

## Giai đoạn 2 — Cronjob (sắp tới)

```bash
# Chạy mỗi ngày 7:00 sáng
0 7 * * * cd /path/to/script && python3 vancouver_job_crawler.py >> logs/crawler.log 2>&1
```
