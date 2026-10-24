# ⚡ me3st (metrest) - Multi-IPTV Fast Checker

**me3st** is a modern, portable, and ultra-fast Python-based tool designed to check and validate multiple IPTV playlists (.m3u) simultaneously using concurrent parallel downloads.

Stop using slow web-based checkers that validate links one by one. **me3st** processes infinite playlists at the same time by leveraging the power of asynchronous programming.

---

## 📥 Download & Quick Start

The easiest way to use **me3st** is to download the latest pre-compiled version. No Python installation required!

1. Go to the **[Latest Release]([https://github.com](https://github.com/MarioSanLabs/me3st/releases))** section.
2. Download the executable file for your operating system.
3. Run it and start checking your playlists in bulk!

*Note: If you prefer to run it as a raw script, check the [Advanced Usage](#-advanced-usage) section below.*

---

## 🚀 Key Features
- **Bulk Checking:** Load as many `.m3u` playlists as you want simultaneously.
- **Ultra Speed:** Concurrent, parallel async validation requests.
- **100% Portable:** Zero heavy interfaces, just pure efficiency.
- **Clean Output:** Automatically exports only the IPTV channels that actually work.

## 🛠️ Advanced Usage (Source Code)

If you want to run the raw `.py` script manually:

1. Clone the repository:
```bash
git clone https://github.com
cd me3st
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the tool:
```bash
python me3st.py
```

## 📈 Roadmap
- [ ] Graphic User Interface (GUI) for desktop.
- [ ] Support for Xtream Codes / dedicated M3U8 formats.
- [ ] Docker support for home servers (NAS/Raspberry Pi).

## 📄 License
This project is licensed under the MIT License.
