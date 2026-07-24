function GeneralMailTemplate(
  mailBody,
  brandSettings = {},
  mailBodyHeading = "INVOICE REMINDER",
  currentSession = {},
  frontEndUrl = "",
  extraData = {}
) {
  const {
    company_logo_color = "",
    company_name = "",
    phone_number = "-",
    support_email = "-",
    website = "#",
    address_line_1 = "",
    primary_color = "#333",
  } = brandSettings;

  const {
    customerName = "Customer",
    docType = "document",
    quoteDetails = "",
    userName = "",
    displayWebsite = website,
    displayEmail = support_email,
    phone = phone_number,
    master_footer_text = "",
    year = new Date().getFullYear(),
  } = extraData;

  // Normalize logo URL
  const normalizedBaseUrl = String(frontEndUrl || "").replace(/\/+$/, "");
  const logoPath = String(company_logo_color || "").trim();

  const logoUrl = (() => {
    if (!logoPath) return "";
    if (/^https?:\/\//i.test(logoPath)) return logoPath;
    if (!normalizedBaseUrl) return "";

    const relativePath = logoPath.startsWith("/") ? logoPath : `/${logoPath}`;
    return `${normalizedBaseUrl}/api/proxy?fileUrl=${encodeURIComponent(relativePath)}`;
  })();

  return `
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>${mailBodyHeading} - ${company_name}</title>
</head>
<body style="margin:0; padding:0; background:#f2f2f2; font-family: Arial, sans-serif;">

  <table width="100%" cellpadding="0" cellspacing="0" style="padding:20px; background-color: #f2f2f2;">
    <tr>
      <td align="center">

        <!-- Main Box -->
        <table width="700" cellpadding="0" cellspacing="0" style="background:#ffffff; border:1px solid #333333;">

          <!-- Header -->
          <tr>
            <td style="padding:25px; border-bottom: 1px solid #eeeeee;">
              <table width="100%" cellpadding="0" cellspacing="0">
                <tr>
                  <!-- Logo -->
                  <td align="left" style="width: 50%;">
                    ${
                      logoUrl
                        ? `<img src="${logoUrl}" alt="${company_name}" style="width: 150px; object-fit: contain; display: block;">`
                        : `<span style="font-size: 20px; font-weight: bold; color: ${primary_color};">${company_name}</span>`
                    }
                  </td>

                  <!-- Title -->
                  <td align="right" style="font-size:16px; font-weight:700; color:${primary_color}; text-transform: uppercase;">
                    ${mailBodyHeading}
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- Content -->
          <tr>
            <td style="padding:40px 35px; font-size:15px; color:#222222; line-height:1.8;">

              <div style="margin-bottom: 25px;">
                
                  <p>Dear ${customerName},</p>

<p>Greetings.</p>

<p>
  This is a reminder that your invoice has been generated as per the agreed schedule.
</p>

<p>
  Please find the attached invoice for your reference. Kindly review the details and ensure the payment is processed on or before the due date.
</p>

<p>
  If you have already completed the payment, please disregard this message.
</p>

<p>
  For any questions or assistance, feel free to contact us. We appreciate your continued business.
</p>

<p>Thank you.</p>
                
              </div>

              ${
                currentSession?.user?.email_signature
                  ? `<div style="margin-top: 35px; border-top: 1px dashed #dddddd; padding-top: 20px;">
                      ${currentSession.user.email_signature}
                    </div>`
                  : `
                    <p style="margin-top:35px; border-top: 1px dashed #dddddd; padding-top: 20px;">
                      Best regards,<br>
                      <strong>${userName}</strong><br>
                      ${company_name}
                    </p>
                  `
              }

            </td>
          </tr>

          <!-- Footer Bar -->
          <tr>
            <td style="background:${primary_color}; color:#ffffff; text-align:center; padding:18px; font-size:13px; font-weight: 500;">
              ${displayWebsite ? `<a href="${website}" style="color: #ffffff; text-decoration: none;">${displayWebsite}</a>` : ""}
              ${displayWebsite && displayEmail ? ` &nbsp; | &nbsp; ` : ""}
              ${displayEmail ? `<a href="mailto:${displayEmail}" style="color: #ffffff; text-decoration: none;">${displayEmail}</a>` : ""}
              ${(displayWebsite || displayEmail) && phone ? ` &nbsp; | &nbsp; ` : ""}
              ${phone ? `<span style="white-space: nowrap;">${phone}</span>` : ""}
            </td>
          </tr>

        </table>

        <!-- Bottom Note -->
        <table width="700" cellpadding="0" cellspacing="0">
          <tr>
            <td align="center" style="font-size:12px; color:#777777; padding-top:15px; padding-bottom: 10px;">
              ${
                master_footer_text
                  ? master_footer_text
                  : `© ${year} ${company_name}. All rights reserved.`
              }
            </td>
          </tr>
        </table>

      </td>
    </tr>
  </table>

</body>
</html>
  `;
}

module.exports = { GeneralMailTemplate };