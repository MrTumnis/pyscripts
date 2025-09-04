import qrcode

data = "2@+pj/0jG8fGiyhM0LKH56Uow76tZk1PEUe/bzmYcCmeBmmZM9RfVJn1UWDHHxCophTTPI9bgWtde6KngnvfX8dchGd78Q1xbHzL8=,eFGT2kYNqb+yub9KsiS++RTAd32HNIWiPAjWApasewY=,JxBt/Q2WAFj55hUWmYIrXCRcjp9/mcZC1z69QBJRolg=,SXzs2sC0kqXth/ol7JRMIb3iG6gV/nP3HpLZlyg4e6U="
img = qrcode.make(data)
img.save("qr_output.png")
