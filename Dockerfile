# استخدام نسخة Node.js الرسمية والمستقرة
FROM node:20

# تحديد مجلد العمل داخل الحاوية
WORKDIR /app

# نسخ ملفات الحزم لتثبيتها
COPY package*.json ./

# تثبيت المكتبات (بما فيها bailey و ioredis و express)
RUN npm install

# نسخ باقي ملفات المشروع إلى الحاوية
COPY . .

# فتح المنفذ (Port) الافتراضي لـ Hugging Face وهو 7860
EXPOSE 7860

# الأمر البرمجي لتشغيل السيرفر
CMD ["node", "app.py"]