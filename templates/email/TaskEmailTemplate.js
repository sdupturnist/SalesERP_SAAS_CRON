function TaskMailTemplate(
  mailBody,
  brandSettings = {},
  mailBodyHeading = "TASK REMINDER",
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
    primary_color = "#2563eb",
  } = brandSettings;

  const {
    customerName = "Customer",
    userName = "",
    displayWebsite = website,
    displayEmail = support_email,
    phone = phone_number,
    master_footer_text = "",
    year = new Date().getFullYear(),
    reminderSentence = "This is a reminder that the following task is due tomorrow.",
  } = extraData;

  // Normalize logo URL
  const normalizedBaseUrl = String(frontEndUrl || "").replace(/\/+$/, "");
  const logoPath = String(company_logo_color || "").trim();

  const logoUrl = (() => {
    if (!logoPath) return "";

    if (/^https?:\/\//i.test(logoPath)) {
      return logoPath;
    }

    if (!normalizedBaseUrl) {
      return "";
    }

    const relativePath = logoPath.startsWith("/")
      ? logoPath
      : `/${logoPath}`;

    return `${normalizedBaseUrl}/api/proxy?fileUrl=${encodeURIComponent(
      relativePath
    )}`;
  })();

  return `
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>${mailBodyHeading} - ${company_name}</title>
</head>

<body
  style="
    margin:0;
    padding:0;
    background:#f4f6f9;
    font-family:Arial, Helvetica, sans-serif;
  "
>

  <table
    width="100%"
    cellpadding="0"
    cellspacing="0"
    style="
      background:#f4f6f9;
      padding:30px 15px;
    "
  >
    <tr>
      <td align="center">

        <!-- Main Container -->
        <table
          width="700"
          cellpadding="0"
          cellspacing="0"
          style="
            width:700px;
            max-width:700px;
            background:#ffffff;
            border:1px solid #e5e7eb;
            border-radius:12px;
            overflow:hidden;
          "
        >

          <!-- Header -->
          <tr>
            <td
              style="
                padding:24px 30px;
                border-bottom:1px solid #eef2f7;
                background:#ffffff;
              "
            >

              <table width="100%" cellpadding="0" cellspacing="0">
                <tr>

                  <!-- Logo -->
                  <td align="left" valign="middle">

                    ${
                      logoUrl
                        ? `
                          <img
                            src="${logoUrl}"
                            alt="${company_name}"
                            style="
                              max-width:160px;
                              height:auto;
                              display:block;
                            "
                          />
                        `
                        : `
                          <span
                            style="
                              font-size:22px;
                              font-weight:700;
                              color:${primary_color};
                            "
                          >
                            ${company_name}
                          </span>
                        `
                    }

                  </td>

                  <!-- Heading -->
                  <td
                    align="right"
                    valign="middle"
                    style="
                      font-size:15px;
                      font-weight:700;
                      color:${primary_color};
                      text-transform:uppercase;
                      letter-spacing:1px;
                    "
                  >
                    ${mailBodyHeading}
                  </td>

                </tr>
              </table>

            </td>
          </tr>

          <!-- Body -->
          <tr>
            <td
              style="
                padding:40px 35px;
                color:#1f2937;
                font-size:15px;
                line-height:1.8;
              "
            >

              <p style="margin-top:0;">
                Dear ${customerName},
              </p>

              <p>
                Greetings.
              </p>

              <p>
                ${reminderSentence}
              </p>

              <!-- Task Details Box -->
              <table
                width="100%"
                cellpadding="0"
                cellspacing="0"
                style="
                  border-collapse:collapse;
                  margin:30px 0;
                  border:1px solid #dbe2ea;
                  border-radius:10px;
                  overflow:hidden;
                  background:#ffffff;
                "
              >

                <!-- Title -->
                <tr>
                  <td
                    colspan="2"
                    style="
                      background:${primary_color};
                      color:#ffffff;
                      padding:16px 20px;
                      font-size:16px;
                      font-weight:700;
                      letter-spacing:0.3px;
                    "
                  >
                    Task Details
                  </td>
                </tr>

                ${mailBody}

              </table>

              <p>
                Please ensure the task is completed before the due date to avoid delays.
              </p>

              <p>
                If you have any questions or need assistance, feel free to contact us.
              </p>

              <p>
                Thank you.
              </p>

              ${
                currentSession?.user?.email_signature
                  ? `
                    <div
                      style="
                        margin-top:35px;
                        border-top:1px dashed #d1d5db;
                        padding-top:20px;
                      "
                    >
                      ${currentSession.user.email_signature}
                    </div>
                  `
                  : `
                    <div
                      style="
                        margin-top:35px;
                        border-top:1px dashed #d1d5db;
                        padding-top:20px;
                        color:#374151;
                      "
                    >
                      Best regards,<br/><br/>

                      <strong>${userName}</strong><br/>
                      ${company_name}
                    </div>
                  `
              }

            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td
              align="center"
              style="
                background:${primary_color};
                padding:20px;
                color:#ffffff;
                font-size:13px;
                line-height:1.8;
              "
            >

              ${
                displayWebsite
                  ? `
                    <a
                      href="${website}"
                      style="
                        color:#ffffff;
                        text-decoration:none;
                        font-weight:500;
                      "
                    >
                      ${displayWebsite}
                    </a>
                  `
                  : ""
              }

              ${
                displayWebsite && displayEmail
                  ? `&nbsp;&nbsp;|&nbsp;&nbsp;`
                  : ""
              }

              ${
                displayEmail
                  ? `
                    <a
                      href="mailto:${displayEmail}"
                      style="
                        color:#ffffff;
                        text-decoration:none;
                        font-weight:500;
                      "
                    >
                      ${displayEmail}
                    </a>
                  `
                  : ""
              }

              ${
                (displayWebsite || displayEmail) && phone
                  ? `&nbsp;&nbsp;|&nbsp;&nbsp;`
                  : ""
              }

              ${
                phone
                  ? `
                    <span style="white-space:nowrap;">
                      ${phone}
                    </span>
                  `
                  : ""
              }

            </td>
          </tr>

        </table>

        <!-- Bottom Copyright -->
        <table
          width="700"
          cellpadding="0"
          cellspacing="0"
          style="max-width:700px;"
        >
          <tr>
            <td
              align="center"
              style="
                padding-top:18px;
                color:#6b7280;
                font-size:12px;
              "
            >

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

module.exports = { TaskMailTemplate };