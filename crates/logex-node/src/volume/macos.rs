use super::*;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStringExt;

pub(super) fn verify(directory: &File, mount: &Path, expected_uuid: &str) -> io::Result<()> {
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: directory fd is live; stat points to aligned writable output.
    if unsafe { libc::fstatfs(directory.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful fstatfs initialized the entire result.
    let stat = unsafe { stat.assume_init() };
    let end = stat
        .f_mntonname
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(|| invalid("filesystem mount path is not terminated"))?;
    let actual = PathBuf::from(std::ffi::OsString::from_vec(
        stat.f_mntonname[..end]
            .iter()
            .map(|byte| *byte as u8)
            .collect(),
    ));
    if actual != mount {
        return Err(invalid(format!(
            "expected mount {mount:?} is not a mounted filesystem (opened filesystem is mounted at {actual:?})"
        )));
    }
    let mut attributes = libc::attrlist {
        bitmapcount: libc::ATTR_BIT_MAP_COUNT,
        reserved: 0,
        commonattr: libc::ATTR_CMN_RETURNED_ATTRS,
        volattr: libc::ATTR_VOL_INFO | libc::ATTR_VOL_UUID,
        dirattr: 0,
        fileattr: 0,
        forkattr: 0,
    };
    // Packed return: length (4), returned attribute_set_t (20), UUID (16).
    // Use u32 alignment, then decode bytes and masks before reading the UUID.
    let mut buffer = [0_u32; 10];
    // SAFETY: attributes describes only the fixed-size fields above; buffer is
    // aligned and writable for its exact supplied length. The live fd identifies
    // the opened filesystem, independently of subsequent pathname changes.
    if unsafe {
        libc::fgetattrlist(
            directory.as_raw_fd(),
            (&mut attributes as *mut libc::attrlist).cast(),
            buffer.as_mut_ptr().cast(),
            std::mem::size_of_val(&buffer),
            0,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    let bytes: Vec<u8> = buffer.iter().flat_map(|word| word.to_ne_bytes()).collect();
    let actual_uuid = decode_uuid(&bytes)?;
    if actual_uuid != expected_uuid {
        return Err(invalid(format!(
            "expected volume UUID {expected_uuid} at {mount:?}, found {actual_uuid}"
        )));
    }
    Ok(())
}

fn decode_uuid(buffer: &[u8]) -> io::Result<String> {
    let word = |offset| {
        buffer
            .get(offset..offset + 4)
            .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
            .map(u32::from_ne_bytes)
    };
    if word(0) != Some(40)
        || word(4).is_none_or(|common| common & libc::ATTR_CMN_RETURNED_ATTRS == 0)
        || word(8).is_none_or(|volume| volume & libc::ATTR_VOL_UUID == 0)
        || buffer.len() < 40
    {
        return Err(invalid("filesystem did not return a supported volume UUID"));
    }
    let uuid = &buffer[24..40];
    if uuid.iter().all(|byte| *byte == 0) {
        return Err(invalid("filesystem returned an empty volume UUID"));
    }
    let mut result = String::with_capacity(36);
    use std::fmt::Write;
    for (index, byte) in uuid.iter().enumerate() {
        if [4, 6, 8, 10].contains(&index) {
            result.push('-');
        }
        write!(result, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response() -> Vec<u8> {
        let mut bytes = vec![0; 40];
        bytes[..4].copy_from_slice(&40_u32.to_ne_bytes());
        bytes[4..8].copy_from_slice(&libc::ATTR_CMN_RETURNED_ATTRS.to_ne_bytes());
        bytes[8..12].copy_from_slice(&libc::ATTR_VOL_UUID.to_ne_bytes());
        bytes[24..].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        bytes
    }

    #[test]
    fn uuid_decode_requires_complete_returned_attributes() {
        let bytes = response();
        assert_eq!(
            decode_uuid(&bytes).unwrap(),
            "01020304-0506-0708-090a-0b0c0d0e0f10"
        );
        for length in 0..40 {
            assert!(decode_uuid(&bytes[..length]).is_err());
        }
        for range in [0..4, 4..8, 8..12, 24..40] {
            let mut invalid = bytes.clone();
            invalid[range].fill(0);
            assert!(decode_uuid(&invalid).is_err());
        }
    }
}
