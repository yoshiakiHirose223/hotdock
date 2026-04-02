const form = document.querySelector("#blog-admin-form");

if (form) {
  const uploadInput = form.querySelector("#upload_file");
  const imageUploadInput = form.querySelector("[data-image-upload-input]");
  const imageUploadStatus = form.querySelector("[data-image-upload-status]");
  const titleInput = form.querySelector("#title");
  const summaryInput = form.querySelector("#summary");
  const slugInput = form.querySelector("#slug");
  const publishedAtInput = form.querySelector("#published_at");
  const markdownInput = form.querySelector("#markdown_source");
  const postIdInput = form.querySelector('input[name="post_id"]');
  const draftKeyInput = form.querySelector("[data-draft-key]");
  const isPublishedInput = form.querySelector("[data-publish-state]");
  const publishToggle = form.querySelector("[data-publish-toggle]");
  const publishToggleLabel = form.querySelector("[data-publish-toggle-label]");
  const sourceFilenameInput = form.querySelector('input[name="current_source_filename"]');
  const importUploadFlag = form.querySelector("[data-import-upload-flag]");
  const deletedImageIdsInput = form.querySelector("[data-deleted-image-ids]");
  const stagedImageManifestInput = form.querySelector("[data-staged-image-manifest]");
  const newTagInput = form.querySelector("[data-new-tag-input]");
  const addTagButton = form.querySelector("[data-add-tag-button]");
  const tagPicker = form.querySelector("[data-tag-picker]");
  const tagEmptyMessage = form.querySelector("[data-tag-empty]");
  const imageLibrary = form.querySelector("[data-image-library]");
  const imageEmptyMessage = form.querySelector("[data-image-empty]");
  const errorPanel = document.querySelector("[data-editor-errors]");
  const previewTitle = document.querySelector("[data-preview-title]");
  const previewSummary = document.querySelector("[data-preview-summary]");
  const previewStatus = document.querySelector("[data-preview-status]");
  const previewDate = document.querySelector("[data-preview-date]");
  const previewTags = document.querySelector("[data-preview-tags]");
  const previewBody = document.querySelector("[data-preview-body]");
  const previewUrl = form.dataset.previewUrl;
  const tagCreateUrl = form.dataset.tagCreateUrl;
  const tagDeleteUrlTemplate = form.dataset.tagDeleteUrlTemplate;

  let previewTimerId = null;
  let previewAbortController = null;

  const safeJsonParse = (value, fallback) => {
    if (!value) {
      return fallback;
    }

    try {
      return JSON.parse(value);
    } catch (error) {
      return fallback;
    }
  };

  const slugify = (value, fallback = "image") => {
    const normalized = value
      .toLowerCase()
      .normalize("NFKD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "");

    return normalized || fallback;
  };

  const createRandomHex = () => {
    const array = new Uint8Array(4);
    window.crypto.getRandomValues(array);
    return Array.from(array, (value) => value.toString(16).padStart(2, "0")).join("");
  };

  const createImageToken = (filename) => {
    const dotIndex = filename.lastIndexOf(".");
    const stem = dotIndex > 0 ? filename.slice(0, dotIndex) : filename;
    return `${slugify(stem, "image")}-${createRandomHex()}`;
  };

  const releasePreviewUrl = (url) => {
    if (typeof url === "string" && url.startsWith("blob:")) {
      URL.revokeObjectURL(url);
    }
  };

  const selectedTagSlugs = () =>
    Array.from(form.querySelectorAll('input[name="selected_tag_slugs"]:checked')).map((input) => input.value);

  const selectedTags = () =>
    Array.from(form.querySelectorAll(".tag-picker-item")).flatMap((item) => {
      const checkbox = item.querySelector('input[name="selected_tag_slugs"]');
      const label = item.querySelector(".tag-toggle span");
      if (!checkbox || !label || !checkbox.checked) {
        return [];
      }
      return [{ slug: checkbox.value, name: label.textContent || "" }];
    });

  const renderErrors = (errors) => {
    if (!errorPanel) {
      return;
    }

    errorPanel.innerHTML = "";
    if (!errors || errors.length === 0) {
      errorPanel.hidden = true;
      return;
    }

    errors.forEach((error) => {
      const line = document.createElement("p");
      line.className = "notice error";
      line.textContent = error;
      errorPanel.appendChild(line);
    });
    errorPanel.hidden = false;
  };

  const renderTagPicker = (tags) => {
    if (!tagPicker || !tagEmptyMessage) {
      return;
    }

    tagPicker.innerHTML = "";

    if (!tags || tags.length === 0) {
      tagEmptyMessage.hidden = false;
      return;
    }

    tagEmptyMessage.hidden = true;

    tags.forEach((tag) => {
      const item = document.createElement("div");
      item.className = "tag-picker-item";

      const label = document.createElement("label");
      label.className = "tag-toggle";

      const checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.name = "selected_tag_slugs";
      checkbox.value = tag.slug;
      checkbox.checked = Boolean(tag.is_selected);

      const text = document.createElement("span");
      text.textContent = tag.name;

      label.appendChild(checkbox);
      label.appendChild(text);

      const deleteButton = document.createElement("button");
      deleteButton.type = "button";
      deleteButton.className = "tag-delete-button";
      deleteButton.dataset.deleteTag = tag.slug;
      deleteButton.dataset.tagName = tag.name;
      deleteButton.textContent = "削除";

      item.appendChild(label);
      item.appendChild(deleteButton);
      tagPicker.appendChild(item);
    });
  };

  const initialPersistedImages = imageLibrary
    ? Array.from(imageLibrary.querySelectorAll(".image-library-card")).flatMap((card) => {
        const idValue = card.getAttribute("data-image-id");
        const token = card.getAttribute("data-image-token") || "";
        const image = card.querySelector("img");
        const name = card.querySelector(".image-library-name");
        const tag = card.querySelector(".image-library-tag");
        const isStaged = card.getAttribute("data-image-staged") === "true";

        if (!idValue || !token || !image || !name || !tag || isStaged) {
          return [];
        }

        return [
          {
            id: Number.parseInt(idValue, 10),
            token,
            url: image.getAttribute("src") || "",
            original_filename: name.textContent || "",
            placeholder_tag: tag.textContent || `[[image:${token}]]`,
            is_staged: false,
          },
        ];
      })
    : [];

  let stagedUploads = safeJsonParse(stagedImageManifestInput?.value || "[]", []).flatMap((item) => {
    if (!item || typeof item !== "object") {
      return [];
    }

    const token = String(item.token || "").trim();
    const originalFilename = String(item.original_filename || "").trim();
    const previewUrl = String(item.preview_url || "").trim();
    if (!token || !originalFilename || !previewUrl) {
      return [];
    }

    return [
      {
        token,
        original_filename: originalFilename,
        preview_url: previewUrl,
        file: null,
      },
    ];
  });

  const deletedPersistedImageIds = new Set(
    safeJsonParse(deletedImageIdsInput?.value || "[]", [])
      .map((value) => Number.parseInt(String(value), 10))
      .filter((value) => Number.isInteger(value) && value > 0),
  );

  const currentImageAssets = () => [
    ...initialPersistedImages.filter((image) => !deletedPersistedImageIds.has(image.id)),
    ...stagedUploads.map((image) => ({
      id: null,
      token: image.token,
      url: image.preview_url,
      original_filename: image.original_filename,
      placeholder_tag: `[[image:${image.token}]]`,
      is_staged: true,
    })),
  ];

  const syncFileInput = () => {
    if (!imageUploadInput || typeof DataTransfer === "undefined") {
      return;
    }

    const dataTransfer = new DataTransfer();
    stagedUploads.forEach((image) => {
      if (image.file) {
        dataTransfer.items.add(image.file);
      }
    });
    imageUploadInput.files = dataTransfer.files;
  };

  const syncImageStateInputs = () => {
    if (deletedImageIdsInput) {
      deletedImageIdsInput.value = JSON.stringify(Array.from(deletedPersistedImageIds));
    }
    if (stagedImageManifestInput) {
      stagedImageManifestInput.value = JSON.stringify(
        stagedUploads.map((image) => ({
          token: image.token,
          original_filename: image.original_filename,
          preview_url: image.preview_url,
        })),
      );
    }
    syncFileInput();
  };

  const updateImageStatus = () => {
    if (!imageUploadStatus) {
      return;
    }

    if (stagedUploads.length === 0) {
      imageUploadStatus.textContent = "画像を選択すると保存前の候補として追加されます。";
      return;
    }

    if (stagedUploads.some((image) => !image.file)) {
      imageUploadStatus.textContent = "未保存の画像があります。保存前に再選択が必要です。";
      return;
    }

    imageUploadStatus.textContent = `未保存の画像: ${stagedUploads.length}件`;
  };

  const renderImageLibrary = () => {
    if (!imageLibrary || !imageEmptyMessage) {
      return;
    }

    imageLibrary.innerHTML = "";
    const images = currentImageAssets();

    if (images.length === 0) {
      imageEmptyMessage.hidden = false;
      updateImageStatus();
      return;
    }

    imageEmptyMessage.hidden = true;

    images.forEach((image) => {
      const card = document.createElement("article");
      card.className = "image-library-card";
      card.dataset.imageToken = image.token;
      if (image.id) {
        card.dataset.imageId = String(image.id);
      }
      if (image.is_staged) {
        card.dataset.imageStaged = "true";
      }

      const preview = document.createElement("img");
      preview.src = image.url;
      preview.alt = image.original_filename;

      const meta = document.createElement("div");
      meta.className = "image-library-meta";

      const name = document.createElement("p");
      name.className = "image-library-name";
      name.textContent = image.original_filename;

      const code = document.createElement("code");
      code.className = "image-library-tag";
      code.textContent = image.placeholder_tag;

      meta.appendChild(name);
      meta.appendChild(code);

      const actions = document.createElement("div");
      actions.className = "image-library-actions";

      const copyButton = document.createElement("button");
      copyButton.type = "button";
      copyButton.className = "secondary";
      copyButton.dataset.copyImageTag = image.placeholder_tag;
      copyButton.textContent = "コピー";

      const deleteButton = document.createElement("button");
      deleteButton.type = "button";
      deleteButton.className = "danger";
      deleteButton.dataset.deleteImageToken = image.token;
      deleteButton.dataset.deleteImageStaged = image.is_staged ? "true" : "false";
      if (image.id) {
        deleteButton.dataset.deleteImageId = String(image.id);
      }
      deleteButton.textContent = "削除";

      actions.appendChild(copyButton);
      actions.appendChild(deleteButton);

      card.appendChild(preview);
      card.appendChild(meta);
      card.appendChild(actions);
      imageLibrary.appendChild(card);
    });

    updateImageStatus();
  };

  const renderPreviewTags = () => {
    if (!previewTags) {
      return;
    }

    previewTags.innerHTML = "";
    selectedTags().forEach((tag) => {
      const chip = document.createElement("span");
      chip.className = "tag-chip";
      chip.textContent = tag.name;
      previewTags.appendChild(chip);
    });
  };

  const syncPreviewMeta = () => {
    if (previewTitle && titleInput) {
      previewTitle.textContent = titleInput.value.trim() || "タイトル未入力";
    }
    if (previewSummary && summaryInput) {
      previewSummary.textContent = summaryInput.value.trim();
      previewSummary.hidden = previewSummary.textContent.length === 0;
    }
    if (previewDate && publishedAtInput) {
      previewDate.textContent = publishedAtInput.value || "";
    }
    renderPreviewTags();
  };

  const applyPreviewStatus = (isPublished) => {
    if (!previewStatus || !isPublishedInput) {
      return;
    }

    isPublishedInput.value = isPublished ? "true" : "false";
    if (publishToggle) {
      publishToggle.checked = isPublished;
    }
    if (publishToggleLabel) {
      publishToggleLabel.textContent = isPublished ? "公開" : "非公開";
    }

    previewStatus.textContent = isPublished ? "公開中" : "未公開";
    previewStatus.classList.toggle("published", isPublished);
    previewStatus.classList.toggle("unpublished", !isPublished);
  };

  const applyEditorState = (editor, { syncFormFields = false } = {}) => {
    if (!editor) {
      return;
    }

    if (draftKeyInput) {
      draftKeyInput.value = editor.draft_key || "";
    }
    if (sourceFilenameInput) {
      sourceFilenameInput.value = editor.source_filename || "";
    }

    if (syncFormFields) {
      if (titleInput) {
        titleInput.value = editor.title || "";
      }
      if (summaryInput) {
        summaryInput.value = editor.summary || "";
      }
      if (slugInput) {
        slugInput.value = editor.slug || "";
      }
      if (markdownInput) {
        markdownInput.value = editor.markdown_source || "";
      }
      if (publishedAtInput) {
        publishedAtInput.value = editor.published_at || "";
      }
      if (newTagInput) {
        newTagInput.value = editor.new_tag_name || "";
      }
      renderTagPicker(editor.available_tags || []);
    }

    syncPreviewMeta();
    applyPreviewStatus(Boolean(editor.is_published));

    if (previewBody) {
      previewBody.innerHTML = editor.preview_html || "<p>本文を入力するとここにプレビューが表示されます。</p>";
    }
  };

  const buildPreviewFormData = ({ includeMarkdownFile = false } = {}) => {
    const formData = new FormData();

    formData.set("title", titleInput ? titleInput.value : "");
    formData.set("summary", summaryInput ? summaryInput.value : "");
    formData.set("slug", slugInput ? slugInput.value : "");
    formData.set("markdown_source", markdownInput ? markdownInput.value : "");
    formData.set("published_at", publishedAtInput ? publishedAtInput.value : "");
    formData.set("new_tag_name", newTagInput ? newTagInput.value : "");
    formData.set("draft_key", draftKeyInput ? draftKeyInput.value : "");
    formData.set("current_source_filename", sourceFilenameInput ? sourceFilenameInput.value : "");
    formData.set("post_id", postIdInput ? postIdInput.value : "");
    formData.set("is_published", isPublishedInput ? isPublishedInput.value : "false");
    formData.set("import_uploaded_file", includeMarkdownFile ? "true" : "false");
    formData.set("deleted_image_ids", deletedImageIdsInput ? deletedImageIdsInput.value : "[]");
    formData.set("staged_image_manifest", stagedImageManifestInput ? stagedImageManifestInput.value : "[]");

    selectedTagSlugs().forEach((slug) => formData.append("selected_tag_slugs", slug));

    if (includeMarkdownFile && uploadInput && uploadInput.files && uploadInput.files[0]) {
      formData.append("upload_file", uploadInput.files[0]);
    }

    return formData;
  };

  const requestPreview = async ({ includeMarkdownFile = false, syncFormFields = false } = {}) => {
    if (!previewUrl) {
      return;
    }

    if (previewAbortController) {
      previewAbortController.abort();
    }
    previewAbortController = new AbortController();

    try {
      const response = await fetch(previewUrl, {
        method: "POST",
        body: buildPreviewFormData({ includeMarkdownFile }),
        signal: previewAbortController.signal,
      });
      const payload = await response.json();

      if (!response.ok) {
        renderErrors(payload.errors || ["プレビュー更新に失敗しました。"]);
        return;
      }

      renderErrors([]);
      applyEditorState(payload.editor, { syncFormFields });
      if (includeMarkdownFile && importUploadFlag) {
        importUploadFlag.value = "false";
      }
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") {
        return;
      }
      renderErrors(["プレビュー更新に失敗しました。"]);
    } finally {
      previewAbortController = null;
    }
  };

  const schedulePreview = () => {
    syncPreviewMeta();
    syncImageStateInputs();
    if (previewTimerId) {
      window.clearTimeout(previewTimerId);
    }
    previewTimerId = window.setTimeout(() => {
      requestPreview({ includeMarkdownFile: false, syncFormFields: false });
    }, 240);
  };

  const requestTagCreate = async () => {
    if (!tagCreateUrl || !newTagInput || !addTagButton) {
      return;
    }

    addTagButton.disabled = true;

    try {
      const formData = new FormData();
      formData.append("tag_name", newTagInput.value);
      selectedTagSlugs().forEach((slug) => formData.append("selected_tag_slugs", slug));

      const response = await fetch(tagCreateUrl, {
        method: "POST",
        body: formData,
      });
      const payload = await response.json();

      if (!response.ok) {
        renderErrors(payload.errors || ["タグ追加に失敗しました。"]);
        return;
      }

      renderErrors([]);
      renderTagPicker(payload.available_tags || []);
      newTagInput.value = "";
      syncPreviewMeta();
    } catch (error) {
      renderErrors(["タグ追加に失敗しました。"]);
    } finally {
      addTagButton.disabled = false;
    }
  };

  const requestTagDelete = async (tagSlug, tagName) => {
    if (!tagDeleteUrlTemplate || !tagSlug) {
      return;
    }
    if (!window.confirm(`タグ ${tagName || tagSlug} を削除しますか？`)) {
      return;
    }

    try {
      const formData = new FormData();
      selectedTagSlugs()
        .filter((slug) => slug !== tagSlug)
        .forEach((slug) => formData.append("selected_tag_slugs", slug));

      const response = await fetch(tagDeleteUrlTemplate.replace("__slug__", tagSlug), {
        method: "POST",
        body: formData,
      });
      const payload = await response.json();

      if (!response.ok) {
        renderErrors(payload.errors || ["タグ削除に失敗しました。"]);
        return;
      }

      renderErrors([]);
      renderTagPicker(payload.available_tags || []);
      syncPreviewMeta();
    } catch (error) {
      renderErrors(["タグ削除に失敗しました。"]);
    }
  };

  const mergeSelectedImages = (fileList, { appendUnmatched = true } = {}) => {
    const pendingFiles = Array.from(fileList || []);
    if (pendingFiles.length === 0) {
      return;
    }

    stagedUploads.forEach((image) => {
      if (image.file || pendingFiles.length === 0) {
        return;
      }

      let matchIndex = pendingFiles.findIndex((file) => file.name === image.original_filename);
      if (matchIndex === -1) {
        matchIndex = 0;
      }

      const [file] = pendingFiles.splice(matchIndex, 1);
      releasePreviewUrl(image.preview_url);
      image.file = file;
      image.original_filename = file.name;
      image.preview_url = URL.createObjectURL(file);
    });

    if (!appendUnmatched) {
      return;
    }

    pendingFiles.forEach((file) => {
      stagedUploads.push({
        token: createImageToken(file.name),
        original_filename: file.name,
        preview_url: URL.createObjectURL(file),
        file,
      });
    });
  };

  const appendSelectedImages = (fileList) => {
    mergeSelectedImages(fileList, { appendUnmatched: true });
    if (stagedUploads.length === 0) {
      return;
    }

    syncImageStateInputs();
    renderImageLibrary();
    renderErrors([]);
    schedulePreview();
  };

  const removeStagedUpload = (token) => {
    const nextUploads = [];
    stagedUploads.forEach((image) => {
      if (image.token === token) {
        releasePreviewUrl(image.preview_url);
        return;
      }
      nextUploads.push(image);
    });
    stagedUploads = nextUploads;
  };

  const removeImageFromEditor = (button) => {
    const token = button.dataset.deleteImageToken || "";
    const isStaged = button.dataset.deleteImageStaged === "true";
    const imageId = Number.parseInt(button.dataset.deleteImageId || "", 10);

    if (!window.confirm("この画像を削除しますか？")) {
      return;
    }

    if (isStaged) {
      removeStagedUpload(token);
    } else if (Number.isInteger(imageId) && imageId > 0) {
      deletedPersistedImageIds.add(imageId);
    }

    syncImageStateInputs();
    renderImageLibrary();
    schedulePreview();
  };

  const copyText = async (text, button) => {
    try {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(text);
      } else {
        const helper = document.createElement("textarea");
        helper.value = text;
        helper.setAttribute("readonly", "");
        helper.style.position = "absolute";
        helper.style.left = "-9999px";
        document.body.appendChild(helper);
        helper.select();
        document.execCommand("copy");
        helper.remove();
      }

      const originalLabel = button.textContent;
      button.textContent = "コピー済み";
      window.setTimeout(() => {
        button.textContent = originalLabel;
      }, 1200);
    } catch (error) {
      renderErrors(["画像タグのコピーに失敗しました。"]);
    }
  };

  if (uploadInput) {
    uploadInput.addEventListener("change", () => {
      if (!uploadInput.files || uploadInput.files.length === 0) {
        return;
      }
      if (importUploadFlag) {
        importUploadFlag.value = "true";
      }
      requestPreview({ includeMarkdownFile: true, syncFormFields: true });
    });
  }

  if (imageUploadInput) {
    imageUploadInput.addEventListener("change", () => {
      if (!imageUploadInput.files || imageUploadInput.files.length === 0) {
        return;
      }
      appendSelectedImages(imageUploadInput.files);
    });
  }

  if (publishToggle) {
    publishToggle.addEventListener("change", () => {
      renderErrors([]);
      applyPreviewStatus(publishToggle.checked);
    });
  }

  if (addTagButton) {
    addTagButton.addEventListener("click", (event) => {
      event.preventDefault();
      requestTagCreate();
    });
  }

  if (newTagInput) {
    newTagInput.addEventListener("keydown", (event) => {
      if (event.key !== "Enter") {
        return;
      }
      event.preventDefault();
      requestTagCreate();
    });
  }

  if (tagPicker) {
    tagPicker.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement) || !target.matches("[data-delete-tag]")) {
        return;
      }

      event.preventDefault();
      requestTagDelete(target.dataset.deleteTag, target.dataset.tagName);
    });

    tagPicker.addEventListener("change", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLInputElement) || target.name !== "selected_tag_slugs") {
        return;
      }
      renderErrors([]);
      syncPreviewMeta();
    });
  }

  if (imageLibrary) {
    imageLibrary.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }

      if (target.matches("[data-copy-image-tag]")) {
        event.preventDefault();
        copyText(target.dataset.copyImageTag || "", target);
        return;
      }

      if (target.matches("[data-delete-image-token]")) {
        event.preventDefault();
        removeImageFromEditor(target);
      }
    });
  }

  if (titleInput) {
    titleInput.addEventListener("input", () => {
      renderErrors([]);
      syncPreviewMeta();
    });
  }

  if (summaryInput) {
    summaryInput.addEventListener("input", () => {
      renderErrors([]);
      syncPreviewMeta();
    });
  }

  if (publishedAtInput) {
    publishedAtInput.addEventListener("change", () => {
      renderErrors([]);
      syncPreviewMeta();
    });
  }

  if (markdownInput) {
    markdownInput.addEventListener("input", () => {
      renderErrors([]);
      schedulePreview();
    });
  }

  form.addEventListener("submit", (event) => {
    if (previewTimerId) {
      window.clearTimeout(previewTimerId);
    }
    if (previewAbortController) {
      previewAbortController.abort();
    }

    if (imageUploadInput && imageUploadInput.files && imageUploadInput.files.length > 0) {
      mergeSelectedImages(imageUploadInput.files, { appendUnmatched: false });
    }

    if (stagedUploads.some((image) => !image.file)) {
      event.preventDefault();
      renderErrors(["保存前の画像を再選択してください。"]);
      syncImageStateInputs();
      renderImageLibrary();
      return;
    }

    if (event.submitter && event.submitter.value === "save_draft") {
      applyPreviewStatus(false);
    }

    syncImageStateInputs();
  });

  syncImageStateInputs();
  renderImageLibrary();
  syncPreviewMeta();
  applyPreviewStatus(isPublishedInput ? isPublishedInput.value === "true" : false);
}
